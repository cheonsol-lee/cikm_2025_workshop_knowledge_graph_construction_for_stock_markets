# 주식 지식 그래프 생성 및 KGQA 시스템

한국 주식 시장 데이터를 Neo4j 기반 지식 그래프로 구축하고, 자연어 질의응답(KGQA) 시스템을 제공하는 프로젝트입니다.

## 📋 목차

- [주요 기능](#주요-기능)
- [시스템 요구사항](#시스템-요구사항)
- [설치 방법](#설치-방법)
- [환경 변수 설정](#환경-변수-설정)
- [사용 방법](#사용-방법)
- [프로젝트 구조](#프로젝트-구조)
- [데이터 스키마](#데이터-스키마)
- [예제](#예제)

## 🚀 주요 기능

### 1. 다중 데이터 소스 통합
- **KRX (한국거래소)**: 상장 회사 기본 정보 수집
- **KIS API (한국투자증권)**: 주가 정보 및 회사 정보 수집
- **MongoDB**: 경쟁사 관계 정보 수집
- **OpenDart API**: 재무제표 정보 수집

### 2. 지식 그래프 구축
- Neo4j를 활용한 주식 도메인 지식 그래프 생성
- 회사, 섹터, 날짜, 주가, 재무제표 등 다양한 엔티티 및 관계 모델링
- 날짜별/분기별/연도별 시계열 데이터 관리

### 3. KGQA (Knowledge Graph Question Answering) 시스템
- LangChain과 OpenAI GPT-4를 활용한 자연어 질의응답
- 자연어 질문을 Cypher 쿼리로 자동 변환
- 지식 그래프 기반 정확한 답변 생성

### 4. 효율적인 데이터 처리
- 날짜 범위별 일괄 처리 지원
- 진행 상황 추적 및 에러 핸들링
- 중단 시 자동 정리 기능

## 💻 시스템 요구사항

- Python 3.8 이상
- Neo4j 4.x 이상
- MongoDB (경쟁사 정보용, 선택사항)

## 📦 설치 방법

### 1. 저장소 클론

```bash
git clone <repository-url>
cd graph_generator
```

### 2. 필요한 패키지 설치

```bash
pip install -r requirements.txt
```

주요 의존성 패키지:
- `neo4j`: Neo4j Python 드라이버
- `langchain`: LLM 체인 관리
- `langchain-openai`: OpenAI 통합
- `pandas`: 데이터 처리
- `requests`: API 호출
- `OpenDartReader`: OpenDart API 클라이언트
- `pymongo`: MongoDB 클라이언트
- `python-dotenv`: 환경 변수 관리
- `tqdm`: 진행률 표시

### 3. Neo4j 데이터베이스 설정

Neo4j 데이터베이스를 설치하고 실행합니다. Docker를 사용하는 경우:

```bash
docker run \
    --name neo4j \
    -p 7474:7474 -p 7687:7687 \
    -e NEO4J_AUTH=neo4j/password \
    -d neo4j:latest
```

## ⚙️ 환경 변수 설정

프로젝트 루트에 `.env` 파일을 생성하고 다음 환경 변수를 설정하세요:

```env
# Neo4j 설정
NEO4J_URI=bolt://localhost:7687
NEO4J_USER=neo4j
NEO4J_PASSWORD=your_password

# OpenAI API 설정
OPENAI_API_KEY=your_openai_api_key

# OpenDart API 설정
OPEN_DART_API_KEY=your_opendart_api_key

# KIS API 설정
KIS_APP_KEY=your_kis_app_key
KIS_APP_SECRET=your_kis_app_secret

# MongoDB 설정 (선택사항)
DB_URI=mongodb://localhost:27017
DB_NAME=your_database_name
DB_COLLECTION_NAME=your_collection_name
```

### API 키 발급 방법

1. **OpenDart API**: [OpenDart 홈페이지](https://opendart.fss.or.kr/)에서 회원가입 후 API 키 발급
2. **KIS API**: [한국투자증권 Open API](https://apiportal.koreainvestment.com/)에서 앱 등록 후 키 발급
3. **OpenAI API**: [OpenAI Platform](https://platform.openai.com/)에서 API 키 발급

## 📖 사용 방법

### 1. 지식 그래프 생성

#### 특정 날짜 처리

```bash
python run_graphdb.py --dates 20230306
```

#### 여러 날짜 처리

```bash
python run_graphdb.py --dates 20230306 20230307 20230308
```

#### 날짜 범위 처리

```bash
python run_graphdb.py --date_st 20230301 --date_fn 20230331
```

#### 설정 파일 사용

`config.json` 파일에 날짜 목록을 정의하고 사용:

```json
{
    "dates": [
        "20230306",
        "20230307",
        "20230308"
    ]
}
```

```bash
python run_graphdb.py --config config.json
```

graph_generator/
├── stock_knowledge_graph.py # Neo4j 지식 그래프 클래스
├── kgqa_system.py # KGQA 시스템 구현
├── run_graphdb.py # 지식 그래프 생성 메인 스크립트
├── data_collectors.py # 다양한 데이터 소스 수집기
├── data_processors.py # 데이터 처리 및 통합
├── graph_builders.py # 그래프 빌더 유틸리티
├── utils.py # 유틸리티 함수
├── clear_database.py # 데이터베이스 관리 스크립트
├── test_kgqa.py # KGQA 테스트 스크립트
├── config_sample.json # 설정 파일 샘플
├── config.json # 실제 설정 파일
└── .env # 환경 변수 파일


```shellscript

# 기존 데이터 삭제 후 새로 생성
python run_graphdb.py --clear --dates 20230306

# 데이터베이스 완전 초기화 (제약조건, 인덱스 포함)
python run_graphdb.py --reset --dates 20230306

# 데이터만 삭제 (그래프 생성 안 함)
python run_graphdb.py --clear-only

# 데이터베이스 완전 초기화만 (그래프 생성 안 함)
python run_graphdb.py --reset-only
```