	DataBase Replication(데이터베이스 복제)
	DB-to-DB로 마이그레이션 가능(Oracle, mssql, postgre, as400등 지원)
	다른 애플리케이션에서 사용하는 데이터베이스와 Mendix 애플리케이션 동기화 가능
	
	step1. DB연결 정보 입력
	step2. table Mapping 작성
	
	체크 완료
	1. Include additional tables 화면의 역할(Inner Join, Outer Join 등)
	2. Import Calls 텝의 역할 -> 스캐줄이벤트로 예약된 마이그레이션 작업을 진행
	
	마지막 정리
	- 확인이 필요한 과제
		1. 비영속성 객체를 지원하는데 언제 사용하며, 어떻게 사용하는가?

	- Module의 보완필요사항
		1. 모듈에 버그가 존재, table Mapping화면에서 Attribute를 선택할 수 없다 -> Microflow 수정 필요

	- Trouble Shooting
		1. 스키마 구조가 존재할 때 Table Mapping시 인스턴스를 찾지 못하는 결함 : PostgreSQL만 확인, 해결방법 찾는중
		2. Import Calls를 정의할 때 제약조건을 걸어주고 Import를 하면 자바액션에서 에러 송출 : 결함 이유 찾는중

  [DB Replication.pptx](https://github.com/kjunho0619/MxDBReplicationExample/files/14520295/DB.Replication.pptx)
