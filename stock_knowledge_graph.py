# stock_knowledge_graph.py
from dotenv import load_dotenv
from neo4j import GraphDatabase
import os
import logging
import warnings
from datetime import datetime
warnings.filterwarnings('ignore')
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger('[Graph DB]')
load_dotenv(dotenv_path=".env")

# 주식 지식그래프 클래스
class StockKnowledgeGraph:
    def __init__(self):
        self.NEO4J_URI = os.getenv("NEO4J_URI")
        self.NEO4J_USER = os.getenv("NEO4J_USER")
        self.NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD")
        self.driver = GraphDatabase.driver(self.NEO4J_URI, auth=(self.NEO4J_USER, self.NEO4J_PASSWORD))

    def close(self):
        self.driver.close()

    # 스키마 추가
    def create_schema(self, cypher_query):
        with self.driver.session() as session:
            session.execute_write(self._create_constraints)
            session.execute_write(self._create_data, cypher_query)

    # 노드 삭제
    def delete_data(self):
        with self.driver.session() as session:
            session.execute_write(self._delete_data)

    # 제약조건 추가 (새로운 스키마에 맞게 수정)
    @staticmethod
    def _create_constraints(tx):
        # Company 제약조건
        tx.run("CREATE CONSTRAINT IF NOT EXISTS FOR (c:Company) REQUIRE c.stock_code IS UNIQUE")
        
        # Date 제약조건
        tx.run("CREATE CONSTRAINT IF NOT EXISTS FOR (d:Date) REQUIRE d.date IS UNIQUE")
        
        # Quarter 제약조건 (year와 quarter의 조합이 유니크)
        tx.run("CREATE CONSTRAINT IF NOT EXISTS FOR (q:Quarter) REQUIRE (q.year, q.quarter) IS UNIQUE")
        
        # Year 제약조건
        tx.run("CREATE CONSTRAINT IF NOT EXISTS FOR (y:Year) REQUIRE y.year IS UNIQUE")
        
        # Sector 제약조건
        tx.run("CREATE CONSTRAINT IF NOT EXISTS FOR (s:Sector) REQUIRE s.stock_sector_nm IS UNIQUE")
        

    # Cypher 쿼리 입력
    @staticmethod
    def _create_data(tx, cypher_query):
        tx.run(cypher_query)

    # 모든 노드와 관계 삭제
    @staticmethod
    def _delete_data(tx):
        tx.run("MATCH (n) DETACH DELETE n")
        print("Knowledge graph schema deleted!")

    # 모든 노드 개수 조회
    def get_node_count(self):
        query = "MATCH (n) RETURN count(n) AS total_node_count"
        with self.driver.session() as session:
            result = session.run(query)
            print(result.single()["total_node_count"])

    def clear_all_data(self):
        """모든 노드와 관계를 삭제"""
        with self.driver.session() as session:
            session.execute_write(self._clear_all_data)
        logger.info("모든 그래프 데이터가 삭제되었습니다.")
    
    def clear_constraints(self):
        """모든 제약조건을 삭제"""
        with self.driver.session() as session:
            session.execute_write(self._clear_constraints)
        logger.info("모든 제약조건이 삭제되었습니다.")
    
    def clear_indexes(self):
        """모든 인덱스를 삭제"""
        with self.driver.session() as session:
            session.execute_write(self._clear_indexes)
        logger.info("모든 인덱스가 삭제되었습니다.")
    
    def reset_database(self):
        """데이터베이스를 완전히 초기화 (모든 데이터, 제약조건, 인덱스 삭제)"""
        logger.info("데이터베이스 초기화를 시작합니다...")
        
        # 1. 모든 데이터 삭제
        self.clear_all_data()
        
        # 2. 모든 제약조건 삭제
        self.clear_constraints()
        
        # 3. 모든 인덱스 삭제
        self.clear_indexes()
        
        logger.info("데이터베이스 초기화가 완료되었습니다.")
    
    def get_database_info(self):
        """데이터베이스 정보 조회"""
        with self.driver.session() as session:
            # 노드 개수 조회
            node_count = session.run("MATCH (n) RETURN count(n) AS node_count").single()["node_count"]
            
            # 관계 개수 조회
            rel_count = session.run("MATCH ()-[r]->() RETURN count(r) AS rel_count").single()["rel_count"]
            
            # 노드 타입별 개수 조회
            node_types = session.run("""
                MATCH (n) 
                RETURN labels(n)[0] AS node_type, count(n) AS count 
                ORDER BY count DESC
            """).data()
            
            # 관계 타입별 개수 조회
            rel_types = session.run("""
                MATCH ()-[r]->() 
                RETURN type(r) AS rel_type, count(r) AS count 
                ORDER BY count DESC
            """).data()
            
            # 제약조건 조회
            constraints = session.run("SHOW CONSTRAINTS").data()
            
            # 인덱스 조회
            indexes = session.run("SHOW INDEXES").data()
            
            return {
                'node_count': node_count,
                'rel_count': rel_count,
                'node_types': node_types,
                'rel_types': rel_types,
                'constraints': constraints,
                'indexes': indexes
            }
        
    @staticmethod
    def _clear_all_data(tx):
        """모든 노드와 관계 삭제"""
        tx.run("MATCH (n) DETACH DELETE n")
    
    @staticmethod
    def _clear_constraints(tx):
        """모든 제약조건 삭제"""
        # 제약조건 목록 조회
        constraints = tx.run("SHOW CONSTRAINTS").data()
        
        for constraint in constraints:
            constraint_name = constraint.get('name')
            if constraint_name:
                try:
                    tx.run(f"DROP CONSTRAINT {constraint_name}")
                except Exception as e:
                    logger.warning(f"제약조건 삭제 실패: {constraint_name}, {e}")
    
    @staticmethod
    def _clear_indexes(tx):
        """모든 인덱스 삭제"""
        # 인덱스 목록 조회
        indexes = tx.run("SHOW INDEXES").data()
        
        for index in indexes:
            index_name = index.get('name')
            if index_name and not index_name.startswith('system'):  # 시스템 인덱스 제외
                try:
                    tx.run(f"DROP INDEX {index_name}")
                except Exception as e:
                    logger.warning(f"인덱스 삭제 실패: {index_name}, {e}")


def _get_date_components(date_str):
    """날짜 문자열에서 년, 월, 일 추출"""
    try:
        if len(date_str) == 8:  # YYYYMMDD 형식
            year = int(date_str[:4])
            month = int(date_str[4:6])
            day = int(date_str[6:8])
        else:  # YYYY-MM-DD 형식
            date_obj = datetime.strptime(date_str, '%Y-%m-%d')
            year = date_obj.year
            month = date_obj.month
            day = date_obj.day
        
        # 분기 계산
        quarter = (month - 1) // 3 + 1
        
        return year, month, day, quarter
    except Exception as e:
        logger.error(f"날짜 파싱 오류: {e}")
        return None, None, None, None

def _create_cypher_query_stock(date, company_dict, stock_price_dict):
    """주식 데이터 생성 쿼리 (새로운 스키마 적용)"""
    year, month, day, quarter = _get_date_components(date)
    
    if year is None:
        logger.error(f"날짜 파싱 실패: {date}")
        return ""
    
    cypher_query = f"""
    // Date 노드 생성
    MERGE (Date:Date {{
        date: '{date}',
        year: {year},
        month: {month},
        day: {day}
    }})
    
    // Quarter 노드 생성
    MERGE (Quarter:Quarter {{
        year: {year},
        quarter: {quarter}
    }})
    
    // Year 노드 생성
    MERGE (Year:Year {{
        year: {year}
    }})
    
    // StockPrice 노드 생성
    MERGE (StockPrice:StockPrice {{
        stck_hgpr: {stock_price_dict['stck_hgpr']}, 
        stck_lwpr: {stock_price_dict['stck_lwpr']}, 
        stck_oprc: {stock_price_dict['stck_oprc']}, 
        stck_clpr: {stock_price_dict['stck_clpr']}
    }})
    
    // Indicator 노드 생성
    MERGE (Indicator:Indicator {{
        pbr: {stock_price_dict.get('pbr', 0)}, 
        per: {stock_price_dict.get('per', 0)}, 
        eps: {stock_price_dict.get('eps', 0)}
    }})
    
    WITH Date, Quarter, Year, StockPrice, Indicator
    MATCH (Company:Company {{stock_code: '{company_dict['stock_code']}'}})
    
    // 새로운 스키마에 맞는 관계 생성
    MERGE (StockPrice)-[:RECORDED_ON]->(Date)
    MERGE (Indicator)-[:MEASURED_ON]->(Date)
    MERGE (Date)-[:IN_QUARTER]->(Quarter)
    MERGE (Quarter)-[:IN_YEAR]->(Year)
    MERGE (Date)-[:IN_YEAR]->(Year)
    MERGE (Company)-[:HAS_STOCK_PRICE]->(StockPrice)
    MERGE (Company)-[:HAS_INDICATOR]->(Indicator)
    """
    
    return cypher_query

def _create_cypher_query_competitor(src, dst):
    """경쟁사 관계 생성 쿼리 (새로운 스키마 적용)"""
    cypher_query = f"""
    // 경쟁사 관계 생성
    MATCH (Company:Company {{stock_code: '{src['stock_code']}'}})
    MATCH (Competitor:Company {{stock_code: '{dst['stock_code']}'}})
    
    // 새로운 스키마에 맞는 관계 생성
    MERGE (Company)-[:COMPETES_WITH]->(Competitor)
    """
    return cypher_query

def _create_cypher_query_company(company_dict):
    """회사 정보만 생성하는 쿼리 (새로운 스키마 적용)"""
    
    # 문자열 이스케이프 처리 함수
    def escape_string(s):
        if s is None:
            return ''
        return str(s).replace("'", "\\'")
    
    # compete_stock_nm_li와 compete_stock_code_li 처리
    compete_stock_nm_li = company_dict.get('compete_stock_nm_li', '')
    compete_stock_code_li = company_dict.get('compete_stock_code_li', '')
    
    # 리스트인 경우 문자열로 변환
    if isinstance(compete_stock_nm_li, list):
        compete_stock_nm_li = ', '.join(compete_stock_nm_li)
    if isinstance(compete_stock_code_li, list):
        compete_stock_code_li = ', '.join(compete_stock_code_li)
    
    cypher_query = f"""
    // Company 노드 생성
    MERGE (Company:Company {{
        stock_code: '{company_dict['stock_code']}', 
        stock_nm: '{escape_string(company_dict['stock_nm'])}', 
        stock_abbrv: '{escape_string(company_dict['stock_abbrv'])}',
        stock_nm_eng: '{escape_string(company_dict['stock_nm_eng'])}',
        listing_dt: '{company_dict['listing_dt']}',
        outstanding_shares: {company_dict['outstanding_shares']},
        kospi200_item_yn: '{company_dict['kospi200_item_yn']}',
        compete_stock_nm_li: '{escape_string(compete_stock_nm_li)}',
        compete_stock_code_li: '{escape_string(compete_stock_code_li)}'
    }})

    // Sector 노드 생성
    MERGE (Sector:Sector {{
        stock_sector_nm: '{escape_string(company_dict['stock_sector_nm'])}'
    }})

    // 새로운 스키마에 맞는 관계 생성
    MERGE (Company)-[:BELONGS_TO]->(Sector)
    """
    return cypher_query

def _create_cypher_query_daily_data(date, company_dict, stock_price_dict, fs_dict=None):
    """일별 데이터 생성 쿼리 (새로운 스키마 적용)"""
    year, month, day, quarter = _get_date_components(date)
    
    if year is None:
        logger.error(f"날짜 파싱 실패: {date}")
        return ""
    
    # 문자열 이스케이프 처리 함수
    def escape_string(s):
        if s is None:
            return ''
        return str(s).replace("'", "\\'")
    
    cypher_query = f"""
    // Date 노드 생성
    MERGE (Date:Date {{
        date: '{date}',
        year: {year},
        month: {month},
        day: {day}
    }})
    
    // Quarter 노드 생성
    MERGE (Quarter:Quarter {{
        year: {year},
        quarter: {quarter}
    }})
    
    // Year 노드 생성
    MERGE (Year:Year {{
        year: {year}
    }})
    
    // StockPrice 노드 생성
    MERGE (StockPrice:StockPrice {{
        stck_hgpr: {stock_price_dict['stck_hgpr']}, 
        stck_lwpr: {stock_price_dict['stck_lwpr']}, 
        stck_oprc: {stock_price_dict['stck_oprc']}, 
        stck_clpr: {stock_price_dict['stck_clpr']}
    }})
    
    // Indicator 노드 생성
    MERGE (Indicator:Indicator {{
        pbr: {stock_price_dict.get('pbr', 0)}, 
        per: {stock_price_dict.get('per', 0)}, 
        eps: {stock_price_dict.get('eps', 0)}
    }})
    
    WITH Date, Quarter, Year, StockPrice, Indicator
    MATCH (Company:Company {{stock_code: '{company_dict['stock_code']}'}})
    """
    
    # 재무제표 데이터가 있는 경우에만 추가
    if fs_dict is not None:
        cypher_query += f"""
    // FinancialStatements 노드 생성
    MERGE (FinancialStatements:FinancialStatements {{
        revenue: {fs_dict.get('revenue', 0)}, 
        operating_income: {fs_dict.get('operating_income', 0)}, 
        net_income: {fs_dict.get('net_income', 0)},
        total_assets: {fs_dict.get('total_assets', 0)}, 
        total_liabilities: {fs_dict.get('total_liabilities', 0)}, 
        total_equity: {fs_dict.get('total_equity', 0)}, 
        capital_stock: {fs_dict.get('capital_stock', 0)}
    }})
    
    WITH Date, Quarter, Year, StockPrice, Indicator, FinancialStatements, Company
    """
    else:
        cypher_query += f"""
    WITH Date, Quarter, Year, StockPrice, Indicator, Company
    """
    
    cypher_query += f"""
    // 새로운 스키마에 맞는 관계 생성
    MERGE (StockPrice)-[:RECORDED_ON]->(Date)
    MERGE (Indicator)-[:MEASURED_ON]->(Date)
    MERGE (Date)-[:IN_QUARTER]->(Quarter)
    MERGE (Quarter)-[:IN_YEAR]->(Year)
    MERGE (Date)-[:IN_YEAR]->(Year)
    MERGE (Company)-[:HAS_STOCK_PRICE]->(StockPrice)
    MERGE (Company)-[:HAS_INDICATOR]->(Indicator)
    """
    
    # 재무제표 관계 추가
    if fs_dict is not None:
        cypher_query += f"""
    MERGE (FinancialStatements)-[:FOR_QUARTER]->(Quarter)
    MERGE (FinancialStatements)-[:FOR_YEAR]->(Year)
    MERGE (Company)-[:HAS_FINANCIAL_STATEMENTS]->(FinancialStatements)
    """
    
    return cypher_query


# 독립 실행을 위한 함수들
def clear_all_graph_data():
    """모든 그래프 데이터 삭제 (독립 실행용)"""
    graph = StockKnowledgeGraph()
    try:
        graph.clear_all_data()
    finally:
        graph.close()

def reset_entire_database():
    """전체 데이터베이스 초기화 (독립 실행용)"""
    graph = StockKnowledgeGraph()
    try:
        graph.reset_database()
    finally:
        graph.close()

def show_database_info():
    """데이터베이스 정보 출력 (독립 실행용)"""
    graph = StockKnowledgeGraph()
    try:
        info = graph.get_database_info()
        
        print("=" * 50)
        print("데이터베이스 정보")
        print("=" * 50)
        print(f"총 노드 개수: {info['node_count']:,}")
        print(f"총 관계 개수: {info['rel_count']:,}")
        
        print("\n노드 타입별 개수:")
        for node_type in info['node_types']:
            print(f"  {node_type['node_type']}: {node_type['count']:,}")
        
        print("\n관계 타입별 개수:")
        for rel_type in info['rel_types']:
            print(f"  {rel_type['rel_type']}: {rel_type['count']:,}")
        
        print(f"\n제약조건 개수: {len(info['constraints'])}")
        print(f"인덱스 개수: {len(info['indexes'])}")
        print("=" * 50)
        
    finally:
        graph.close()