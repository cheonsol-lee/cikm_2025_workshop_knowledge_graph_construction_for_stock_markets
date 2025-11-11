# kgqa_system.py
from langchain.chains import LLMChain
from langchain.prompts import PromptTemplate
from langchain_openai import ChatOpenAI
from stock_knowledge_graph import StockKnowledgeGraph
import os
from dotenv import load_dotenv

load_dotenv(dotenv_path=".env")

class StockKGQASystem:
    """주식 지식그래프 QA 시스템"""
    
    def __init__(self):
        # 환경 변수 로드
        self.NEO4J_URI = os.getenv("NEO4J_URI")
        self.NEO4J_USER = os.getenv("NEO4J_USER")
        self.NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD")
        self.OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
        
        # 그래프 및 LLM 초기화
        self.stock_kg = StockKnowledgeGraph()
        self.llm = ChatOpenAI(openai_api_key=self.OPENAI_API_KEY, temperature=0, model="gpt-4.1")
        
        # 새로운 스키마 정의
        self.stock_schema = self.stock_schema = """
Node types:
- Company
    - stock_code: 종목코드 (예: 000660)
    - stock_nm: 회사명 (예: SK하이닉스)
    - stock_abbrv: 종목명 (예: SK하이닉스)
    - stock_nm_eng: 영문종목명
    - listing_dt: 상장일
    - outstanding_shares: 발행주식수
    - kospi200_item_yn: KOSPI200 여부
    - compete_stock_nm_li: 경쟁사명 리스트
    - compete_stock_code_li: 경쟁사코드 리스트
- Sector
    - stock_sector_nm: 업종명 (예: 반도체)
- Date
    - date: 날짜 (예: 20250913)
    - year: 년도 (예: 2025)
    - month: 월 (예: 9)
    - day: 일 (예: 13)
- Quarter
    - year: 년도 (예: 2025)
    - quarter: 분기 (예: 3)
- Year
    - year: 년도 (예: 2025)
- StockPrice
    - stck_hgpr: 최고가
    - stck_lwpr: 최저가
    - stck_oprc: 시가
    - stck_clpr: 종가
- Indicator
    - pbr: 주가자산비율
    - per: 주가수익비율
    - eps: 주당순이익
- FinancialStatements
    - revenue: 매출액
    - operating_income: 영업이익
    - net_income: 순이익
    - total_assets: 총자산
    - total_liabilities: 총부채
    - total_equity: 총자본
    - capital_stock: 자본금

Relationships:
- (StockPrice)-[:RECORDED_ON]->(Date)
- (Indicator)-[:MEASURED_ON]->(Date)
- (Date)-[:IN_QUARTER]->(Quarter)
- (Quarter)-[:IN_YEAR]->(Year)
- (Date)-[:IN_YEAR]->(Year)
- (FinancialStatements)-[:FOR_QUARTER]->(Quarter)
- (FinancialStatements)-[:FOR_YEAR]->(Year)
- (Company)-[:HAS_STOCK_PRICE]->(StockPrice)
- (Company)-[:HAS_INDICATOR]->(Indicator)
- (Company)-[:HAS_FINANCIAL_STATEMENTS]->(FinancialStatements)
- (Company)-[:BELONGS_TO]->(Sector)
- (Company)-[:COMPETES_WITH]->(Company)
"""
        
        # 프롬프트 템플릿 설정
        self._setup_prompts()
    
    def _setup_prompts(self):
        """프롬프트 템플릿 설정"""
        # Cypher 쿼리 생성 프롬프트
        self.cypher_prompt = PromptTemplate(
            input_variables=["schema", "question"],
            template="""
            다음 스키마를 기반으로 Cypher 쿼리를 생성하세요:

            스키마:
            {schema}

            질문: {question}

            다음 규칙을 따라주세요:
            1. MATCH, WHERE, RETURN 절을 사용하세요
            2. 노드와 관계의 라벨을 정확히 사용하세요
            3. 속성명을 정확히 사용하세요 (stock_abbrv, stock_code 등)
            4. 날짜는 "YYYYMMDD" 형식의 문자열로 사용하세요 (예: "20230306")
            5. 관계 방향을 정확히 사용하세요:
            - (StockPrice)-[:RECORDED_ON]->(Date)
            - (Indicator)-[:MEASURED_ON]->(Date)
            - (Company)-[:HAS_STOCK_PRICE]->(StockPrice)
            - (Company)-[:HAS_INDICATOR]->(Indicator)
            - (Company)-[:HAS_FINANCIAL_STATEMENTS]->(FinancialStatements)
            6. 쿼리만 반환하고 다른 설명은 하지 마세요

            Cypher 쿼리:
            """
        )

        # 답변 생성 프롬프트
        self.answer_prompt = PromptTemplate(
            input_variables=["question", "cypher_query", "query_result"],
            template="""
            다음 정보를 바탕으로 질문에 답변하세요:

            질문: {question}
            Cypher 쿼리: {cypher_query}
            쿼리 결과: {query_result}

            자연스러운 한국어로 답변하세요:
            """
        )

        # 종목명 추출 프롬프트
        self.stock_name_prompt = PromptTemplate(
            input_variables=["query"],
            template="""
            문장에서 종목명(stock_abbrv)만 정확히 추출해 주세요.
            단, 반드시 종목명만 한 줄로 출력해 주세요. 불필요한 설명이나 다른 정보는 포함하지 마세요.
            만약, 종목명이 없으면 '없음'으로 출력해 주세요.

            예를들면, 종목명이 있는 문장은 다음과 같습니다.
            <query>
                SK하이닉스의 경쟁사를 알려주고, 경쟁사의 pbr, per, bps, eps을 알려줘"
            </query>
            <output>
                SK하이닉스
            </output>

            예를들면, 종목명이 없는 문장은 다음과 같습니다.
            <query>
                안녕하세요. 종목명을 입력해주세요.
            </query>
            <output>
                없음
            </output>

            다음 문장에서 종목명(stock_abbrv)만 정확히 추출해 주세요.
            <query>
                {query}
            </query>
            
            <output>
            </output>
            """
        )
        
        # LLM 체인 생성
        self.cypher_chain = LLMChain(llm=self.llm, prompt=self.cypher_prompt)
        self.answer_chain = LLMChain(llm=self.llm, prompt=self.answer_prompt)
        self.stock_name_chain = LLMChain(llm=self.llm, prompt=self.stock_name_prompt)
    
    def get_stock_name_by_query(self, query: str):
        """질문에서 종목명 추출"""
        stock_name = self.stock_name_chain.run(query=query)
        return stock_name.strip()
    
    def stock_kgqa(self, question: str):
        """StockKnowledgeGraph를 사용한 Knowledge Graph QA"""
        try:
            # 1. Cypher 쿼리 생성
            cypher_query = self.cypher_chain.run(schema=self.stock_schema, question=question)
            cypher_query = cypher_query.strip()
            
            # 2. 마크다운 코드 블록 제거
            if cypher_query.startswith('```cypher'):
                cypher_query = cypher_query.replace('```cypher', '').replace('```', '').strip()
            elif cypher_query.startswith('```'):
                cypher_query = cypher_query.replace('```', '').strip()
            
            # 3. StockKnowledgeGraph에서 쿼리 실행
            with self.stock_kg.driver.session() as session:
                result = session.run(cypher_query)
                query_result = [record.data() for record in result]
            
            # 4. 답변 생성
            answer = self.answer_chain.run(
                question=question,
                cypher_query=cypher_query,
                query_result=str(query_result)
            )
            
            return {
                'answer': answer.strip(),
                'cypher': cypher_query,
                'query_result': query_result
            }
        except Exception as e:
            return {
                'answer': f"오류가 발생했습니다: {str(e)}",
                'cypher': '',
                'query_result': []
            }
    
    def close(self):
        """연결 종료"""
        self.stock_kg.close()