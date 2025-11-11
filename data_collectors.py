# data_collectors.py
import requests
import json
import os
import pandas as pd
import OpenDartReader
from pymongo import MongoClient
from datetime import datetime, timedelta
import logging
from dotenv import load_dotenv

load_dotenv(dotenv_path=".env")
logger = logging.getLogger('[Data Collectors]')

class KISTokenManager:
    """KIS API 토큰 관리 클래스"""
    
    def __init__(self, app_key, app_secret):
        self.app_key = app_key
        self.app_secret = app_secret
        self.token_path = "kis_access_token.dat"
        self.token_info_path = "kis_token_info.json"
    
    def _is_token_valid(self, token_info):
        """토큰이 유효한지 확인"""
        if not token_info:
            return False
        
        # 토큰 만료 시간 확인 (24시간 후 만료)
        created_time = token_info.get('created_time')
        if not created_time:
            return False
        
        from datetime import datetime, timedelta
        created_dt = datetime.fromisoformat(created_time)
        expiry_dt = created_dt + timedelta(hours=23)  # 23시간 후 만료 (여유시간)
        
        return datetime.now() < expiry_dt
    
    def _load_token_info(self):
        """저장된 토큰 정보 로드"""
        if os.path.exists(self.token_info_path):
            try:
                with open(self.token_info_path, "r") as f:
                    return json.load(f)
            except Exception as e:
                logger.warning(f"토큰 정보 파일 읽기 실패: {e}")
        return None
    
    def _save_token_info(self, token, created_time):
        """토큰 정보 저장"""
        token_info = {
            'token': token,
            'created_time': created_time.isoformat()
        }
        try:
            with open(self.token_info_path, "w") as f:
                json.dump(token_info, f)
            logger.info(f"토큰 정보가 {self.token_info_path}에 저장되었습니다.")
        except Exception as e:
            logger.warning(f"토큰 정보 파일 저장 실패: {e}")
    
    def get_access_token(self, force_refresh=False):
        """KIS API 액세스 토큰을 가져오거나 생성"""
        # 강제 갱신이 아닌 경우 기존 토큰 확인
        if not force_refresh:
            token_info = self._load_token_info()
            if token_info and self._is_token_valid(token_info):
                logger.info("기존 KIS 토큰을 사용합니다.")
                return token_info['token']
        
        # 새 토큰 요청
        logger.info("새로운 KIS 토큰을 요청합니다.")
        url = "https://openapi.koreainvestment.com:9443/oauth2/tokenP"
        headers = {"Content-Type": "application/json"}
        data = {
            "grant_type": "client_credentials",
            "appkey": self.app_key,
            "appsecret": self.app_secret,
        }
        
        try:
            res = requests.post(url, headers=headers, data=json.dumps(data))
            
            if res.status_code != 200:
                logger.error(f"Failed to get token: {res.status_code}, {res.text}")
                return None
                
            response_data = res.json()
            new_token = response_data.get("access_token")
            
            if new_token:
                # 토큰 정보 저장
                created_time = datetime.now()
                self._save_token_info(new_token, created_time)
                
                # 기존 토큰 파일도 업데이트 (호환성을 위해)
                try:
                    with open(self.token_path, "w") as f:
                        f.write(new_token)
                except Exception as e:
                    logger.warning(f"토큰 파일 저장 실패: {e}")
                
                logger.info("새 토큰이 생성되었습니다.")
                return new_token
            else:
                logger.error("토큰 응답에서 access_token을 찾을 수 없습니다.")
                return None
                
        except Exception as e:
            logger.error(f"토큰 요청 중 오류 발생: {e}")
            return None
    
    def refresh_token(self):
        """토큰 강제 갱신"""
        return self.get_access_token(force_refresh=True)

class KRXDataCollector:
    """KRX 데이터 수집 클래스"""
    
    @staticmethod
    def get_company_info():
        """KRX에서 회사 정보 수집"""
        url = 'https://data.krx.co.kr/comm/bldAttendant/getJsonData.cmd'
        headers = {
            'User-Agent': 'Mozilla/5.0',
            'Referer': 'http://data.krx.co.kr/',
            'Content-Type': 'application/x-www-form-urlencoded'
        }
        data = {
            'bld': 'dbms/MDC/STAT/standard/MDCSTAT01901',
            'mktId': 'ALL',
            'share': '1',
            'csvxls_isNo': 'false'
        }

        res = requests.post(url, headers=headers, data=data)
        res.encoding = 'utf-8-sig'
        json_data = res.json()
        
        df = pd.DataFrame(json_data['OutBlock_1'])
        df['ISU_SRT_CD'] = df['ISU_SRT_CD'].apply(lambda x: x.zfill(6))
        df['LIST_DD'] = pd.to_datetime(df['LIST_DD'])
        df['LIST_SHRS'] = df['LIST_SHRS'].str.replace(',', '').astype(int)

        col_li = ['ISU_SRT_CD', 'ISU_NM', 'ISU_ABBRV', 'ISU_ENG_NM', 'LIST_DD', 'MKT_TP_NM', 'LIST_SHRS']
        df = df[col_li]
        df = df.rename(columns={
            'ISU_SRT_CD': 'stock_code',
            'ISU_NM': 'stock_nm',
            'ISU_ABBRV': 'stock_abbrv',
            'ISU_ENG_NM': 'stock_nm_eng',
            'LIST_DD': 'listing_dt',
            'MKT_TP_NM': 'market_nm',
            'LIST_SHRS': 'outstanding_shares'
        })
        return df

class KISDataCollector:
    """KIS API 데이터 수집 클래스"""
    
    def __init__(self, app_key, app_secret, access_token):
        self.app_key = app_key
        self.app_secret = app_secret
        self.access_token = access_token
    
    def get_company_info(self, stock_code):
        """KIS에서 회사 정보 수집"""
        url = "https://openapi.koreainvestment.com:9443/uapi/domestic-stock/v1/quotations/search-stock-info"
        headers = {
            "Content-Type": "application/json",
            "authorization": f"Bearer {self.access_token}",
            "appkey": self.app_key,
            "appsecret": self.app_secret,
            "tr_id": "CTPF1002R",
            "custtype": "P",
        }
        params = {
            "PRDT_TYPE_CD": "300",
            "PDNO": stock_code
        }

        res = requests.get(url, headers=headers, params=params)
        data = res.json()
        
        if data.get("rt_cd") != "0":
            logger.error(f"[{stock_code}] 오류 발생: {data.get('msg1')}")
            return None
        if not data:
            logger.error(f"[{stock_code}] 데이터가 없습니다.")
            return None
            
        df = pd.DataFrame([data['output']])
        df["stock_code"] = stock_code
        df = df[['stock_code', 'kospi200_item_yn', 'std_idst_clsf_cd_name']]
        df = df.rename(columns={'std_idst_clsf_cd_name': 'stock_sector_nm'})
        return df
    
    def get_price_info(self, stock_code, date_st, date_fn):
        """KIS에서 주가 정보 수집"""
        url = "https://openapi.koreainvestment.com:9443/uapi/domestic-stock/v1/quotations/inquire-daily-itemchartprice"
        headers = {
            "Content-Type": "application/json",
            "authorization": f"Bearer {self.access_token}",
            "appkey": self.app_key,
            "appsecret": self.app_secret,
            "tr_id": "FHKST03010100",
            "custtype": "P"
        }
        params = {
            "FID_COND_MRKT_DIV_CODE": "J",
            "FID_INPUT_ISCD": stock_code,
            "FID_INPUT_DATE_1": date_st,
            "FID_INPUT_DATE_2": date_fn,
            "FID_PERIOD_DIV_CODE": "D",
            "FID_ORG_ADJ_PRC": 1
        }
        
        try:
            res = requests.get(url, headers=headers, params=params)
            data = res.json()
            
            if data.get("rt_cd") != "0":
                logger.error(f"[{stock_code}] 조회 실패: {data.get('msg1')}")
                return None
            if not data:
                logger.error(f"[{stock_code}] 데이터가 없습니다.")
                return None

            # output2가 존재하고 비어있지 않은지 확인
            if 'output2' not in data or not data['output2'] or len(data['output2']) == 0:
                logger.debug(f"[{stock_code}] 주가 데이터가 없습니다. (output2 비어있음)")
                return None
                
            # output1이 존재하는지 확인
            if 'output1' not in data:
                logger.debug(f"[{stock_code}] 기본 정보가 없습니다. (output1 없음)")
                return None

            try:
                price_dict = {
                    'stock_code': stock_code,
                    'date': date_st,
                    'stck_hgpr': data['output2'][0].get('stck_hgpr', 0),
                    'stck_lwpr': data['output2'][0].get('stck_lwpr', 0),
                    'stck_oprc': data['output2'][0].get('stck_oprc', 0),
                    'stck_clpr': data['output2'][0].get('stck_clpr', 0),
                    'eps': data['output1'].get('eps', 0),
                    'pbr': data['output1'].get('pbr', 0),
                    'per': data['output1'].get('per', 0)
                }
                return pd.DataFrame([price_dict])
                
            except (KeyError, IndexError) as e:
                logger.error(f"[{stock_code}] 데이터 파싱 오류: {e}")
                return None
                
        except Exception as e:
            logger.error(f"[{stock_code}] API 호출 오류: {e}")
            return None

class MongoDBCollector:
    """MongoDB 데이터 수집 클래스"""
    
    @staticmethod
    def get_competitor_info(db_uri, db_name, collection_name):
        """MongoDB에서 경쟁사 정보 수집"""
        try:
            client = MongoClient(db_uri)
            db = client[db_name]
            collection = db[collection_name]
            documents = collection.find()
            data = list(documents)
            
            if data:
                competitor_df = pd.DataFrame(data)
                logger.info("Convert MongoDB to competitor_df")
                
                # target_company 정보 추출
                competitor_df['stock_code'] = competitor_df['target_company'].apply(
                    lambda target: target['code'] if target and 'code' in target else None
                )
                competitor_df['stock_name'] = competitor_df['target_company'].apply(
                    lambda target: target['name'] if target and 'name' in target else None
                )
                
                # 경쟁사 코드와 이름을 모두 추출 (competitors 컬럼에서)
                competitor_df['compete_stock_code_li'] = competitor_df['competitors'].apply(
                    lambda comp_list: [comp['code'] for comp in comp_list if 'code' in comp]
                )
                competitor_df['compete_stock_nm_li'] = competitor_df['competitors'].apply(
                    lambda comp_list: [comp.get('name', '') for comp in comp_list if 'name' in comp]
                )
                
                # 필요한 컬럼만 선택
                competitor_df = competitor_df[[
                    'stock_code', 'stock_name',
                    'compete_stock_code_li', 'compete_stock_nm_li'
                ]]
                return competitor_df
            else:
                logger.info("No data in collection")
                return pd.DataFrame(columns=[
                    'stock_code', 'stock_name',
                    'compete_stock_code_li', 'compete_stock_nm_li'
                ])
        except Exception as e:
            logger.error(f"Failed to DB connection: {e}")
            return pd.DataFrame(columns=[
                'stock_code', 'stock_name',
                'compete_stock_code_li', 'compete_stock_nm_li'
            ])
        finally:
            client.close()

class OpenDartCollector:
    """OpenDart API 데이터 수집 클래스"""
    
    def __init__(self, api_key):
        self.api_key = api_key
    
    def get_financial_statements(self, stock_code, date):
        """OpenDart에서 재무제표 정보 수집"""
        def _get_quarter_list(date):
            year = int(date[:4])
            month = int(date[4:6])
        
            if month in [1, 2, 3]:
                quarters = [(year-1, '11011', '4')]
            elif month in [4, 5, 6]:
                quarters = [(year, '11013', '1'), (year-1, '11011', '4')]
            elif month in [7, 8, 9]:
                quarters = [(year, '11012', '2'), (year, '11013', '1'), (year-1, '11011', '4')]
            else:
                quarters = [(year, '11014', '3'), (year, '11012', '2'), (year, '11013', '1'), (year-1, '11011', '4')]
            return quarters

        dart = OpenDartReader(self.api_key)
        col_nm_li = ['매출액', '영업이익', '당기순이익', '자산총계', '부채총계', '자본총계', '자본금']
        col_eng_li = ['revenue', 'operating_income', 'net_income', 'total_assets', 'total_liabilities', 'total_equity', 'capital_stock']

        for bsns_year, reprt_code, quarter_nm in _get_quarter_list(date):
            try:
                logger.debug(f"Financial Statements: (stock_code: {stock_code}, year: {bsns_year}, quarter: {quarter_nm})")
                dart_df = dart.finstate(corp=stock_code, bsns_year=str(bsns_year), reprt_code=reprt_code)

                if dart_df is None or len(dart_df) == 0:
                    continue

                fs_info = []
                for col_nm in col_nm_li:
                    try:
                        value = dart_df[(dart_df['account_nm'] == col_nm) & (dart_df['fs_nm'] == '연결재무제표')]['thstrm_amount'].values
                        if len(value) == 0:
                            value = dart_df[(dart_df['account_nm'] == col_nm) & (dart_df['fs_nm'] == '재무제표')]['thstrm_amount'].values
                        fs_info.append(int(value[0].replace(',', '')) if len(value) > 0 else 0)
                    except Exception as e:
                        fs_info.append(0)

                fs_df = pd.DataFrame([fs_info], columns=col_eng_li)
                fs_df['year'] = bsns_year
                fs_df['quarter'] = quarter_nm
                fs_df['stock_code'] = stock_code
                fs_df = fs_df[['stock_code', 'year', 'quarter'] + col_eng_li]
                return fs_df

            except Exception as e:
                continue

        # 모든 분기 시도 실패 시 0으로 채워진 DataFrame 반환
        logger.debug(f"No available financial data for {stock_code}")
        fs_df = pd.DataFrame([[0] * len(col_eng_li)], columns=col_eng_li)
        fs_df['year'] = bsns_year
        fs_df['quarter'] = quarter_nm
        fs_df['stock_code'] = stock_code
        fs_df = fs_df[['stock_code', 'year', 'quarter'] + col_eng_li]
        return fs_df

def get_date_list(date_st, date_fn):
    """날짜 리스트 생성"""
    start_date = datetime.strptime(date_st, '%Y%m%d')
    end_date = datetime.strptime(date_fn, '%Y%m%d')
    
    date_li = []
    current_date = start_date
    while current_date <= end_date:
        date_li.append(current_date.strftime('%Y%m%d'))
        current_date += timedelta(days=1)
    return date_li