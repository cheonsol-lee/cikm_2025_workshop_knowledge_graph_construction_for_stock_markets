# graph_builders.py
import logging
import pandas as pd
from stock_knowledge_graph import _create_cypher_query_stock, _create_cypher_query_competitor

logger = logging.getLogger('[Graph Builders]')

def get_competitor_info(stock_code, graph_df):
    """경쟁사 정보 추출"""
    try:
        # source: stock_code에 해당하는 회사 정보 저장
        src_company_dict = graph_df[graph_df['stock_code'] == stock_code].iloc[0].to_dict()
        
        # destination: 경쟁사 데이터 추출
        compete_stock_code_li = graph_df[graph_df['stock_code'] == stock_code].compete_stock_code_li.values[0]
        
        # compete_stock_code_li가 None이거나 빈 값인 경우 처리
        if not compete_stock_code_li or compete_stock_code_li == [] or compete_stock_code_li == '':
            return src_company_dict, []
        
        # compete_stock_code_li가 문자열인 경우 리스트로 변환
        if isinstance(compete_stock_code_li, str):
            compete_stock_code_li = [code.strip() for code in compete_stock_code_li.split(',') if code.strip()]
        
        # compete_stock_code_li가 리스트가 아닌 경우 빈 리스트 반환
        if not isinstance(compete_stock_code_li, list):
            return src_company_dict, []
        
        # 빈 리스트인 경우 빈 리스트 반환
        if len(compete_stock_code_li) == 0:
            return src_company_dict, []
        
        dst_company_dict_li = []
        for compete_stock_code in compete_stock_code_li:
            try:
                # source 회사 제외
                if stock_code == compete_stock_code:
                    continue
                dst_company_dict = graph_df[graph_df['stock_code'] == compete_stock_code].iloc[0].to_dict()
                dst_company_dict_li.append(dst_company_dict)
            except Exception as e:
                continue
        
        return src_company_dict, dst_company_dict_li
        
    except Exception as e:
        # 오류 발생 시 빈 리스트 반환
        return {}, []

def create_graph_db(graph, graph_df, stock_code, date_li):
    """그래프 DB 생성"""
    try:
        filter_df = graph_df[graph_df['stock_code'] == stock_code]
        company_dict = filter_df.iloc[0].to_dict()
        
        # 1. 주식 데이터 추가
        for i, date in enumerate(date_li):
            try:
                stock_price_dict = filter_df[filter_df['date'] == date].iloc[0].to_dict()
                cypher_stock_query = _create_cypher_query_stock(
                    date, company_dict, stock_price_dict
                )
                graph.create_schema(cypher_stock_query)
            except Exception as e:
                logger.error(f"Error: {company_dict['stock_nm']} 추가 불가: {e}")
                continue

        # 2. 경쟁사 데이터 추가
        try:
            src_company_dict, dst_company_dict_li = get_competitor_info(stock_code, graph_df)
            for dst_company_dict in dst_company_dict_li:
                cypher_competitor_query = _create_cypher_query_competitor(src_company_dict, dst_company_dict)
                graph.create_schema(cypher_competitor_query)
        except Exception as e:
            logger.error(f"Error: 경쟁사 데이터 추가 불가: {e}")

    except Exception as e:
        logger.error(f"Error: 주식 데이터 생성 불가: {e}")
    
    finally:
        graph.close()