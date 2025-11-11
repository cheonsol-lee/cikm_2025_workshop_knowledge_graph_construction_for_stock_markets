# data_processors.py
import pandas as pd
import numpy as np
import time
import logging
from tqdm import tqdm
from data_collectors import (
    KISTokenManager, KRXDataCollector, KISDataCollector, 
    MongoDBCollector, OpenDartCollector
)
from utils import measure_time

logger = logging.getLogger('[Data Processors]')

class StockDataProcessor:
    """주식 데이터 처리 클래스"""
    
    def __init__(self, date_li):
        self.sleep_sec = 0.1
        self.date_li = date_li
        
        # 환경 변수 로드
        import os
        from dotenv import load_dotenv
        load_dotenv(dotenv_path=".env")
        
        self.OPEN_DART_API_KEY = os.getenv("OPEN_DART_API_KEY")
        self.DB_URI = os.getenv("DB_URI")
        self.DB_NAME = os.getenv("DB_NAME")
        self.DB_COLLECTION_NAME = os.getenv("DB_COLLECTION_NAME")
        self.KIS_APP_KEY = os.getenv("KIS_APP_KEY")
        self.KIS_APP_SECRET = os.getenv("KIS_APP_SECRET")
        
        # 데이터 수집기 초기화
        self.kis_token_manager = KISTokenManager(self.KIS_APP_KEY, self.KIS_APP_SECRET)
        self.kis_access_token = self.kis_token_manager.get_access_token()
        self.kis_collector = KISDataCollector(self.KIS_APP_KEY, self.KIS_APP_SECRET, self.kis_access_token)
        self.krx_collector = KRXDataCollector()
        self.mongodb_collector = MongoDBCollector()
        self.dart_collector = OpenDartCollector(self.OPEN_DART_API_KEY)
        
        # 데이터 저장소 초기화
        self.company_df = None
        self.company_df_krx = None
        self.company_df_kis = None
        self.price_df = None
        self.competitor_df = None
        self.fs_df = None
        self.total_df = None

    @measure_time
    def get_company_info(self):
        """회사 정보 수집 및 처리"""
        logger.info("[1. get_company_info...]")
        
        # KRX 데이터 수집
        self.company_df_krx = self.krx_collector.get_company_info()
        stock_code_li = self.company_df_krx.stock_code

        # KIS 데이터 수집
        company_kis_li = []
        for stock_code in tqdm(stock_code_li, desc='Collect kis company info'):
            company_kis = self.kis_collector.get_company_info(stock_code)
            if company_kis is not None:
                company_kis_li.append(company_kis)
            time.sleep(self.sleep_sec)

        self.company_df_kis = pd.concat(company_kis_li, ignore_index=True)
        self.company_df_kis['stock_sector_nm'].replace('', np.nan, inplace=True)
        self.company_df_kis.fillna('없음', inplace=True)
        self.company_df = pd.merge(self.company_df_krx, self.company_df_kis, how='left', on='stock_code')

    @measure_time
    def get_price_info(self):
        """주가 정보 수집 및 처리"""
        logger.info("[2. get_price_info...]")
        
        # company_df_krx가 없으면 에러 발생
        if not hasattr(self, 'company_df_krx') or self.company_df_krx is None:
            logger.error("company_df_krx가 없습니다. get_company_info()를 먼저 실행하세요.")
            return

        stock_code_li = self.company_df_krx.stock_code
        price_kis_li = []
        for date in self.date_li:
            for stock_code in tqdm(stock_code_li, desc=f'Collect kis price info (date: {date})'):
                price_kis = self.kis_collector.get_price_info(stock_code, date, date)
                if price_kis is not None:
                    price_kis_li.append(price_kis)
                time.sleep(self.sleep_sec)

        self.price_df = pd.concat(price_kis_li, ignore_index=True)

    @measure_time
    def get_competitor_info(self):
        """경쟁사 정보 수집 및 처리"""
        logger.info("[3. get_competitor_info...]")
        self.competitor_df = self.mongodb_collector.get_competitor_info(
            self.DB_URI, self.DB_NAME, self.DB_COLLECTION_NAME
        )
        print(f'self.competitor_df.shape: {self.competitor_df.shape}')

        # company_df_krx가 없으면 에러 발생
        if not hasattr(self, 'company_df_krx') or self.company_df_krx is None:
            logger.error("company_df_krx가 없습니다. get_company_info()를 먼저 실행하세요.")
            return
        
        # company_df_krx에 존재하는 stock_code만 필터링
        valid_stock_codes = set(self.company_df_krx.stock_code)
        self.competitor_df = self.competitor_df[
            self.competitor_df['stock_code'].isin(valid_stock_codes)
        ]
        
        # company_df_krx에 있지만 competitor_df에 없는 stock_code 추가
        existing_codes = set(self.competitor_df['stock_code'])
        missing_stock_codes = valid_stock_codes - existing_codes
        
        if missing_stock_codes:
            missing_df = pd.DataFrame({
                'stock_code': list(missing_stock_codes),
                'stock_name': [None] * len(missing_stock_codes),
                'compete_stock_code_li': [[]] * len(missing_stock_codes),
                'compete_stock_nm_li': [[]] * len(missing_stock_codes)
            })
            self.competitor_df = pd.concat([self.competitor_df, missing_df], ignore_index=True)

    @measure_time
    def get_financial_statements(self):
        """재무제표 정보 수집 및 처리"""
        logger.info("[4. get_financial_statements...]")
        
        # company_df_krx가 없으면 에러 발생
        if not hasattr(self, 'company_df_krx') or self.company_df_krx is None:
            logger.error("company_df_krx가 없습니다. get_company_info()를 먼저 실행하세요.")
            return
            
        date = self.date_li[0]
        stock_code_li = self.company_df_krx.stock_code
        fs_li = []
        for stock_code in tqdm(stock_code_li, desc=f'Collect financial statements info (date: {date})'):
            fs = self.dart_collector.get_financial_statements(stock_code, date)
            if fs is not None:
                fs_li.append(fs)
            time.sleep(self.sleep_sec)

        self.fs_df = pd.concat(fs_li, ignore_index=True)

    def create_total_df(self):
        """종합 데이터프레임 생성 (새로운 스키마에 맞게 수정)"""
        logger.info("[5. create_total_df...]")
    
        self.total_df = pd.merge(self.company_df, self.price_df, on='stock_code', how='left')
        self.total_df = pd.merge(self.total_df, self.competitor_df, on='stock_code', how='left')
        self.total_df = pd.merge(self.total_df, self.fs_df, on='stock_code', how='left')
        
        return self.total_df

    @measure_time
    def run_all(self):
        """전체 데이터 수집 및 처리 실행"""
        self.get_company_info()
        self.get_price_info()
        self.get_competitor_info()
        self.get_financial_statements()
        return self.create_total_df()