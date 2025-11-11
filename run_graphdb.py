# run_graphdb.py
from utils import measure_time
from tqdm import tqdm
from stock_knowledge_graph import StockKnowledgeGraph
from data_processors import StockDataProcessor
from data_collectors import get_date_list
from graph_builders import create_graph_db
import argparse
import logging
from datetime import datetime
import json
import os
import time
import signal
import sys
import pandas as pd

# 로그 레벨을 WARNING으로 설정하여 INFO 로그 제거
logging.basicConfig(level=logging.ERROR, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
# Neo4j 관련 로거들 비활성화
logging.getLogger('neo4j.notifications').setLevel(logging.ERROR)
logging.getLogger('neo4j').setLevel(logging.ERROR)
logging.getLogger('neo4j.bolt').setLevel(logging.ERROR)

logger = logging.getLogger('[Graph DB]')

# 전역 변수로 그래프 객체와 처리된 날짜들을 추적
current_graph = None
processed_dates = []
current_date = None

def signal_handler(signum, frame):
    """시그널 핸들러 (Ctrl+C 등)"""
    print(f"\n\n⚠️  중단 신호를 받았습니다 (Signal: {signum})")
    print("그래프 생성을 중단하고 정리 작업을 시작합니다...")
    cleanup_and_exit()

def cleanup_and_exit():
    """정리 작업 및 종료"""
    global current_graph, processed_dates, current_date
    
    if current_graph:
        try:
            print("방금 처리된 데이터를 정리 중...")
            
            if processed_dates:
                print(f"처리된 날짜들: {processed_dates}")
                print("해당 날짜의 데이터를 제거 중...")
                cleanup_processed_data(current_graph, processed_dates)
            
            if current_date:
                print(f"현재 처리 중이던 날짜 {current_date}의 데이터를 제거 중...")
                cleanup_single_date_data(current_graph, current_date)
            
            print("정리 작업이 완료되었습니다.")
            
        except Exception as e:
            print(f"정리 작업 중 오류 발생: {e}")
        finally:
            try:
                current_graph.close()
                print("그래프 연결이 종료되었습니다.")
            except:
                pass
    
    print("프로그램을 종료합니다.")
    sys.exit(1)

def cleanup_processed_data(graph, dates):
    """처리된 날짜들의 데이터 제거"""
    try:
        with graph.driver.session() as session:
            for date in dates:
                # 해당 날짜의 StockPrice, Indicator, FinancialStatements 노드 제거
                cleanup_query = f"""
                MATCH (d:Date {{date: '{date}'}})
                OPTIONAL MATCH (d)-[:RECORDED_ON]->(sp:StockPrice)
                OPTIONAL MATCH (sp)-[:HAS_STOCK_PRICE]->(i:Indicator)
                OPTIONAL MATCH (i)-[:HAS_INDICATOR]->(fs:FinancialStatements)
                DETACH DELETE sp, i, fs
                DELETE d
                """
                session.run(cleanup_query)
                print(f"  {date} 날짜 데이터 제거 완료")
    except Exception as e:
        print(f"데이터 제거 중 오류 발생: {e}")

def cleanup_single_date_data(graph, date):
    """단일 날짜의 데이터 제거"""
    try:
        with graph.driver.session() as session:
            cleanup_query = f"""
            MATCH (d:Date {{date: '{date}'}})
            OPTIONAL MATCH (d)-[:RECORDED_ON]->(sp:StockPrice)
            OPTIONAL MATCH (sp)-[:HAS_STOCK_PRICE]->(i:Indicator)
            OPTIONAL MATCH (i)-[:HAS_INDICATOR]->(fs:FinancialStatements)
            DETACH DELETE sp, i, fs
            DELETE d
            """
            session.run(cleanup_query)
            print(f"  {date} 날짜 데이터 제거 완료")
    except Exception as e:
        print(f"데이터 제거 중 오류 발생: {e}")

def parse_args():
    parser = argparse.ArgumentParser(description='Generate knowledge graph of stock domain')
    parser.add_argument('--dates', type=str, nargs='+', help='Specific dates (format: YYYYMMDD)')
    parser.add_argument('--date_st', type=str, help='Start date (format: YYYYMMDD)')
    parser.add_argument('--date_fn', type=str, help='Finish date (format: YYYYMMDD)')
    parser.add_argument('--config', type=str, help='Config file path for predefined dates')
    parser.add_argument('--clear', action='store_true', help='Clear all data before building')
    parser.add_argument('--reset', action='store_true', help='Reset entire database before building')
    parser.add_argument('--clear-only', action='store_true', help='Clear data only, do not build graph')
    parser.add_argument('--reset-only', action='store_true', help='Reset database only, do not build graph')
    args = parser.parse_args()
    
    # 날짜 형식 검증 (삭제 전용 옵션인 경우 제외)
    if not args.clear_only and not args.reset_only:
        if args.dates:
            for date in args.dates:
                try:
                    datetime.strptime(date, '%Y%m%d')
                except ValueError:
                    parser.error(f"Invalid date format: {date}. Use YYYYMMDD format")
        elif args.date_st and args.date_fn:
            try:
                datetime.strptime(args.date_st, '%Y%m%d')
                datetime.strptime(args.date_fn, '%Y%m%d')
            except ValueError:
                parser.error("Follow date format (format: YYYYMMDD)")
        elif args.config:
            if not os.path.exists(args.config):
                parser.error(f"Config file not found: {args.config}")
        else:
            parser.error("Please provide either --dates, --date_st/--date_fn, or --config")
    
    return args

def load_predefined_dates(config_path):
    """미리 정의된 날짜 목록 로드"""
    with open(config_path, 'r') as f:
        config = json.load(f)
    return config.get('dates', [])

def get_target_dates(args):
    """대상 날짜 목록 생성"""
    if args.dates:
        return args.dates
    elif args.config:
        return load_predefined_dates(args.config)
    else:
        return get_date_list(args.date_st, args.date_fn)

def format_time(seconds):
    """초 단위 시간을 시:분:초 형태로 변환 (utils.py와 동일)"""
    hours = int(seconds // 3600)
    minutes = int((seconds % 3600) // 60)
    seconds = int(seconds % 60)
    return f"{hours:02d}:{minutes:02d}:{seconds:02d}"

@measure_time
def process_single_date(date, stock_processor, graph, existing_companies):
    """단일 날짜 처리 함수 (measure_time 데코레이터 적용)"""
    global current_date, processed_dates
    
    current_date = date
    
    # 각 날짜별 세부 진행단계를 위한 서브 프로그레스 바
    date_steps = ["주가 정보 수집", "재무제표 정보 수집", "그래프 데이터 추가"]
    date_pbar = tqdm(date_steps, desc=f"{date} 처리", position=1, leave=False,
                   bar_format='{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}]')
    
    success_count = 0
    error_count = 0
    
    try:
        for step in date_steps:
            if step == "주가 정보 수집":
                date_pbar.set_description(f"{date}: {step}")
                date_processor = StockDataProcessor([date])
                # 기존 stock_processor의 데이터를 복사
                date_processor.company_df = stock_processor.company_df
                date_processor.company_df_krx = stock_processor.company_df_krx 
                date_processor.company_df_kis = stock_processor.company_df_kis
                date_processor.get_price_info()
                date_pbar.set_postfix({"상태": "완료"})
                
            elif step == "재무제표 정보 수집":
                date_pbar.set_description(f"{date}: {step}")
                date_processor.get_financial_statements()
                date_pbar.set_postfix({"상태": "완료"})
                
            elif step == "그래프 데이터 추가":
                date_pbar.set_description(f"{date}: {step}")
                if date_processor.price_df is not None and not date_processor.price_df.empty:
                    success_count, error_count = add_daily_data_to_graph(
                        graph, date_processor.company_df, 
                        date_processor.price_df, date_processor.fs_df, date, existing_companies
                    )
                    date_pbar.set_postfix({"성공": success_count, "실패": error_count})
                else:
                    date_pbar.set_postfix({"상태": "데이터 없음"})
                    success_count, error_count = 0, 0
            
            date_pbar.update(1)
    
    except KeyboardInterrupt:
        print(f"\n⚠️  {date} 처리 중 중단되었습니다.")
        raise
    except Exception as e:
        print(f"\n❌ {date} 처리 중 오류 발생: {e}")
        raise
    finally:
        date_pbar.close()
        
        # 성공적으로 완료된 경우에만 processed_dates에 추가
        if success_count > 0 or error_count == 0:
            processed_dates.append(date)
            current_date = None
    
    return success_count, error_count

@measure_time
def main(args):
    global current_graph, processed_dates, current_date
    
    # 시그널 핸들러 등록
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    try:
        # 삭제 전용 옵션 처리
        if args.clear_only:
            print("그래프 데이터 삭제를 시작합니다...")
            graph = StockKnowledgeGraph()
            try:
                graph.clear_all_data()
                print("그래프 데이터 삭제가 완료되었습니다.")
            finally:
                graph.close()
            return
        
        if args.reset_only:
            print("데이터베이스 전체 초기화를 시작합니다...")
            graph = StockKnowledgeGraph()
            try:
                graph.reset_database()
                print("데이터베이스 초기화가 완료되었습니다.")
            finally:
                graph.close()
            return
        
        # 데이터베이스 초기화 옵션 처리
        if args.reset:
            print("데이터베이스 전체 초기화를 시작합니다...")
            graph = StockKnowledgeGraph()
            try:
                graph.reset_database()
            finally:
                graph.close()
            print("데이터베이스 초기화가 완료되었습니다.")
        elif args.clear:
            print("그래프 데이터 삭제를 시작합니다...")
            graph = StockKnowledgeGraph()
            try:
                graph.clear_all_data()
            finally:
                graph.close()
            print("그래프 데이터 삭제가 완료되었습니다.")

        # 대상 날짜 목록 생성
        target_dates = get_target_dates(args)
        print(f"대상 날짜: {target_dates}")
        
        # 그래프 초기화
        current_graph = StockKnowledgeGraph()
        
        try:
            # 1. 회사 정보 및 섹터 정보 수집 (한 번만)
            print("\n=== 1단계: 회사 정보 수집 ===")
            print("회사, 경쟁사 정보 수집 중...")
            stock_processor = StockDataProcessor(target_dates)

            # run_all 함수를 사용하여 모든 데이터 수집 및 total_df 생성
            print("전체 데이터 수집 및 처리 중...")
            total_df = stock_processor.run_all()
            print(total_df.columns)
            print("회사, 경쟁사 정보 수집 완료")
            
            # 2. 기존 그래프에서 회사 정보 확인
            print("\n=== 2단계: 기존 회사 정보 확인 ===")
            print("기존 회사 정보 확인 중...")
            existing_companies = check_existing_companies(current_graph)
            print(f"기존 회사 수: {len(existing_companies)}")
            
            # 3. 새로운 회사들만 추가 (Sector, Company 노드)
            print("\n=== 3단계: 새 회사 및 섹터 추가 ===")
            new_companies = []
            for _, company in total_df.iterrows():
                if company['stock_code'] not in existing_companies:
                    new_companies.append(company)
            
            if new_companies:
                print(f"새 회사 {len(new_companies)}개 추가 중...")
                add_companies_to_graph(current_graph, new_companies)
                print("새 회사 추가 완료")
            else:
                print("추가할 새 회사가 없습니다.")
            
            # 4. 경쟁사 관계 추가 (한 번만)
            print("\n=== 4단계: 경쟁사 관계 추가 ===")
            print("경쟁사 관계 추가 중...")

            # competitor_df가 None이 아닌지 확인
            if stock_processor.competitor_df is not None and not stock_processor.competitor_df.empty:
                add_competitor_relationships(current_graph, total_df, stock_processor.competitor_df)
                print("경쟁사 관계 추가 완료")
            else:
                print("경쟁사 데이터가 없어 경쟁사 관계 추가를 건너뜁니다.")
            
            # 5. 각 날짜별로 주가 및 재무제표 정보 수집
            print(f"\n=== 5단계: 날짜별 데이터 처리 ({len(target_dates)}개 날짜) ===")
            
            # 전체 날짜 진행률을 위한 메인 프로그레스 바
            main_pbar = tqdm(target_dates, desc="전체 날짜 처리", position=0, leave=True, 
                            bar_format='{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}, {rate_fmt}]')
            
            for i, date in enumerate(main_pbar, 1):
                main_pbar.set_description(f"전체 진행 [{i}/{len(target_dates)}]")
                
                try:
                    # 각 날짜별 처리 (measure_time 데코레이터가 자동으로 시간 측정)
                    success_count, error_count = process_single_date(date, stock_processor, current_graph, existing_companies)
                    
                    # 메인 프로그레스 바에 결과 표시
                    main_pbar.set_postfix({
                        "성공": success_count, 
                        "실패": error_count,
                        "현재": date
                    })
                    
                except KeyboardInterrupt:
                    print(f"\n⚠️  {date} 처리 중 중단되었습니다.")
                    raise
                except Exception as e:
                    print(f"\n❌ {date} 처리 중 오류 발생: {e}")
                    raise
            
            main_pbar.close()
            
            print(f"\n=== 모든 날짜 처리 완료! ({len(target_dates)}개 날짜) ===")
            print("각 날짜별 상세 시간은 위의 로그를 참조하세요.")
            
        except KeyboardInterrupt:
            print(f"\n⚠️  그래프 생성이 중단되었습니다.")
            cleanup_and_exit()
        except Exception as e:
            print(f"\n❌ 그래프 생성 중 오류 발생: {e}")
            cleanup_and_exit()
        finally:
            if current_graph:
                current_graph.close()
                current_graph = None
    
    except KeyboardInterrupt:
        print(f"\n⚠️  프로그램이 중단되었습니다.")
        cleanup_and_exit()
    except Exception as e:
        print(f"\n❌ 예상치 못한 오류 발생: {e}")
        cleanup_and_exit()

def check_existing_companies(graph):
    """기존 그래프에서 회사 목록 확인"""
    query = "MATCH (c:Company) RETURN c.stock_code as stock_code"
    with graph.driver.session() as session:
        result = session.run(query)
        return [record['stock_code'] for record in result]

def add_companies_to_graph(graph, companies):
    """회사 정보를 그래프에 추가 (Sector 노드도 함께 생성)"""
    from stock_knowledge_graph import _create_cypher_query_company
    
    for company in companies:
        try:
            cypher_query = _create_cypher_query_company(company.to_dict())
            graph.create_schema(cypher_query)
        except Exception as e:
            logger.error(f"Error adding company {company['stock_abbrv']}: {e}")

def add_competitor_relationships(graph, total_df, competitor_df):
    """경쟁사 관계 추가 (한 번만 실행)"""
    from stock_knowledge_graph import _create_cypher_query_competitor
    from graph_builders import get_competitor_info
    
    # 기존 경쟁사 관계 확인
    existing_relationships = check_existing_competitor_relationships(graph)
    print(f"기존 경쟁사 관계 수: {len(existing_relationships)}")
    
    # competitor_df가 None이거나 비어있는 경우 처리
    if competitor_df is None or competitor_df.empty:
        print("경쟁사 데이터가 없어 경쟁사 관계를 추가하지 않습니다.")
        return
    
    # total_df가 이미 병합되어 있으므로 compete_stock_code_li 컬럼 확인
    if 'compete_stock_code_li' not in total_df.columns:
        print("total_df에 compete_stock_code_li 컬럼이 없습니다.")
        return

    # compete_stock_code_li 컬럼의 NaN 값을 빈 리스트로 채우기
    total_df['compete_stock_code_li'] = total_df['compete_stock_code_li'].fillna('')
    
    added_relationships = 0
    for _, company in total_df.iterrows():
        try:
            # compete_stock_code_li가 있는 경우에만 처리
            compete_codes = company.get('compete_stock_code_li', '')
            if not compete_codes or compete_codes == [] or compete_codes == '':
                continue
                
            src_company_dict, dst_company_dict_li = get_competitor_info(company['stock_code'], total_df)

            # dst_company_dict_li가 비어있으면 건너뛰기
            if not dst_company_dict_li:
                continue
            
            for dst_company_dict in dst_company_dict_li:
                # 관계가 이미 존재하는지 확인
                relationship_key = f"{company['stock_code']}-{dst_company_dict['stock_code']}"
                if relationship_key not in existing_relationships:
                    cypher_competitor_query = _create_cypher_query_competitor(src_company_dict, dst_company_dict)
                    graph.create_schema(cypher_competitor_query)
                    added_relationships += 1
                    logger.info(f"경쟁사 관계 추가: {company['stock_code']} -> {dst_company_dict['stock_code']}")
                else:
                    logger.info(f"경쟁사 관계 이미 존재: {company['stock_code']} -> {dst_company_dict['stock_code']}")
                    
        except Exception as e:
            logger.error(f"Error adding competitor relationships for {company.get('stock_abbrv', 'Unknown')}: {e}")
    
    print(f"총 {added_relationships}개의 경쟁사 관계가 추가되었습니다.")

def check_existing_competitor_relationships(graph):
    """기존 경쟁사 관계 확인"""
    query = """
    MATCH (c1:Company)-[:COMPETES_WITH]->(c2:Company) 
    RETURN c1.stock_code as src_code, c2.stock_code as dst_code
    """
    with graph.driver.session() as session:
        result = session.run(query)
        relationships = set()
        for record in result:
            relationships.add(f"{record['src_code']}-{record['dst_code']}")
        return relationships

def add_daily_data_to_graph(graph, company_df, price_df, fs_df, date, existing_companies):
    """일별 데이터를 그래프에 추가 (StockPrice, Indicator, FinancialStatements만)"""
    from stock_knowledge_graph import _create_cypher_query_daily_data
    
    success_count = 0
    error_count = 0
    
    for _, company in company_df.iterrows():
        stock_code = company['stock_code']
        
        # 해당 날짜의 주가 데이터 찾기
        price_data = price_df[price_df['stock_code'] == stock_code]
        if price_data.empty:
            error_count += 1
            continue
            
        # 해당 회사의 재무제표 데이터 찾기
        fs_data = fs_df[fs_df['stock_code'] == stock_code] if fs_df is not None else None
        
        try:
            cypher_query = _create_cypher_query_daily_data(
                date, company.to_dict(), price_data.iloc[0].to_dict(), 
                fs_data.iloc[0].to_dict() if fs_data is not None and not fs_data.empty else None
            )
            graph.create_schema(cypher_query)
            success_count += 1
        except Exception as e:
            error_count += 1
            logger.error(f"Error adding daily data for {company['stock_abbrv']} on {date}: {e}")
    
    return success_count, error_count

if __name__ == "__main__":
    args = parse_args()
    main(args)