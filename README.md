	DataBase Replication(데이터베이스 복제)
	DB-to-DB로 마이그레이션 가능(Oracle, mssql, postgre, as400등 지원)
	다른 애플리케이션에서 사용하는 데이터베이스와 Mendix 애플리케이션 동기화 가능
	
	step1. DB연결 정보 입력
	step2. table Mapping 작성
	
	체크 완료
	1. Include additional tables 화면의 역할(Inner Join, Outer Join 등)
	2. Import Calls 텝의 역할 -> 스캐줄이벤트로 예약된 마이그레이션 작업을 진행
	
	앞으로 해야 될 과제
	1. 비영속성 객체를 지원하는데 어떤방식으로 할 수 있는가?
	
	Module Supplementary matters
	1. 모듈의 버그가 존재, table Mapping화면에서 Attribute를 선택할 수 없음 -> Microflow 수정필요
	
	Trouble Shooting
	1. 스키마 구조 입력 시 table Mapping에서 인스턴스를 찾지 못하는 결함 : Postgresql만 확인함. 다른 DB에선 될 수 있음
	2. Import Calls 제약조건을 걸어 줬을 때 자바액션 에러 결함 : 결함 찾는중
