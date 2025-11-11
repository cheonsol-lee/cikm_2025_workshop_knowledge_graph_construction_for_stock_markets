# utils.py
import time
import logging
from datetime import datetime

logger = logging.getLogger('[Utils]')

def measure_time(func):
    """함수 실행 시간을 측정하는 데코레이터"""
    def format_time(seconds):
        """초 단위 시간을 시:분:초 형태로 변환"""
        hours = int(seconds // 3600)
        minutes = int((seconds % 3600) // 60)
        seconds = int(seconds % 60)
        log_str = f"{hours:02d}:{minutes:02d}:{seconds:02d}"
        return log_str

    def wrapper(*args, **kwargs):
        start_time = time.time()
        logger.info(f"----------------------------------------------------------------------")
        logger.info(f"Start time: {datetime.fromtimestamp(start_time).strftime('%Y-%m-%d %H:%M:%S')}")
        
        # 함수 실행
        result = func(*args, **kwargs)
        
        end_time = time.time()
        total_elapsed_time = end_time - start_time
        logger.info(f"End time: {datetime.fromtimestamp(end_time).strftime('%Y-%m-%d %H:%M:%S')}")
        logger.info(f"Total Time: {format_time(total_elapsed_time)}")
        return result
    return wrapper