# test_kgqa.py
from kgqa_system import StockKGQASystem

def test_kgqa():
    """KGQA 시스템 테스트"""
    print("=== 새로운 스키마를 사용한 KGQA 테스트 ===")
    
    # KGQA 시스템 초기화
    kgqa_system = StockKGQASystem()
    
    # 테스트 질문들
    test_questions = [
        "Analyze the performance trends by comparing the revenue, operating income, and net income of Samsung Electronics (stock code: 005930) with its competitor SK Hynix (stock code: 000660) for the years 2023, 2024, and 2025.",
        "Within the same industry as SK Hynix, identify companies with high growth potential or undervalued stocks based on PER, PBR, and EPS for the years 2023, 2024, and 2025."
        # "2023-03-06의 마이크로컨텍솔의 주가를 알려줘",
        # "GS의 경쟁사를 알려주고, 경쟁사의 pbr, per, eps을 알려줘",
        # "20230306 날짜의 마이크로컨텍솔의 주가를 알려줘",
        # "삼성전자의 경쟁사를 알려주고, 경쟁사의 pbr, per, eps을 알려줘",
        # "삼성전자의 재무제표 정보를 알려줘",
        # "2025년 3분기 삼성전자의 정보를 알려줘"
    ]

    for i, question in enumerate(test_questions, 1):
        print(f"\n--------- 테스트 {i} ---------")
        
        # 종목명 추출
        stock_name = kgqa_system.get_stock_name_by_query(question)
        
        # KGQA 실행
        result = kgqa_system.stock_kgqa(question)

        print(f"- 질문: {question}")
        print(f"- 추출된 종목명: {stock_name}")
        print(f"- 답변: {result['answer']}")
        print(f"- Cypher 쿼리: {result['cypher']}")
        print(f"- 쿼리 결과: {result['query_result']}")
        print("-" * 50)
    
    # 연결 정리
    kgqa_system.close()
    print("StockKnowledgeGraph 연결이 종료되었습니다.")

if __name__ == "__main__":
    test_kgqa()