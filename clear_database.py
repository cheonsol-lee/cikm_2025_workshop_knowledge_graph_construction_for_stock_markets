# clear_database.py
import argparse
import logging
from stock_knowledge_graph import (
    clear_all_graph_data, 
    reset_entire_database, 
    show_database_info,
    StockKnowledgeGraph
)

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger('[Clear Database]')

def parse_args():
    parser = argparse.ArgumentParser(description='Clear Neo4j database')
    parser.add_argument('--action', type=str, required=True, 
                       choices=['clear', 'reset', 'info', 'confirm-clear', 'confirm-reset'],
                       help='Action to perform: clear (data only), reset (everything), info (show info)')
    parser.add_argument('--force', action='store_true', 
                       help='Skip confirmation prompt')
    return parser.parse_args()

def confirm_action(action):
    """사용자 확인 프롬프트"""
    if action in ['confirm-clear', 'confirm-reset']:
        return True
    
    print(f"\n⚠️  경고: '{action}' 작업을 수행하려고 합니다.")
    print("이 작업은 되돌릴 수 없습니다!")
    
    if action == 'clear':
        print("- 모든 노드와 관계가 삭제됩니다.")
        print("- 제약조건과 인덱스는 유지됩니다.")
    elif action == 'reset':
        print("- 모든 노드와 관계가 삭제됩니다.")
        print("- 모든 제약조건이 삭제됩니다.")
        print("- 모든 인덱스가 삭제됩니다.")
    
    response = input("\n정말로 계속하시겠습니까? (yes/no): ").lower()
    return response in ['yes', 'y']

def main():
    args = parse_args()
    
    if args.action == 'info':
        show_database_info()
        return
    
    if not args.force and not confirm_action(args.action):
        print("작업이 취소되었습니다.")
        return
    
    try:
        if args.action in ['clear', 'confirm-clear']:
            logger.info("그래프 데이터 삭제를 시작합니다...")
            clear_all_graph_data()
            logger.info("그래프 데이터 삭제가 완료되었습니다.")
            
        elif args.action in ['reset', 'confirm-reset']:
            logger.info("데이터베이스 초기화를 시작합니다...")
            reset_entire_database()
            logger.info("데이터베이스 초기화가 완료되었습니다.")
        
        # 삭제 후 정보 출력
        print("\n삭제 후 데이터베이스 정보:")
        show_database_info()
        
    except Exception as e:
        logger.error(f"오류 발생: {e}")
        raise

if __name__ == "__main__":
    main()