# ADR 0002: Podman 연동에 SDK 대신 os/exec 기반 CLI 호출 사용

- Status: Accepted
- Date: 2026-03-30

## Context

이 프로젝트는 실행 중인 컨테이너 안에서 `radosgw-admin` 명령을 호출해야 한다.

초기 범위에서는 `bucket-stats` 유스케이스 하나만 지원하면 되고, 컨테이너 생성이나 생명주기 관리까지 다루지 않는다.

Podman 연동 방식을 정할 때 다음 조건이 중요하다.

- 외부 Podman API 라이브러리 의존성을 최소화해야 한다.
- 구현은 단순하고 추적 가능해야 한다.
- 실행 중인 컨테이너에 대한 명령 위임만 할 수 있으면 된다.
- 샘플 출력 JSON을 읽어 도메인 모델로 매핑하는 수준부터 빠르게 시작할 수 있어야 한다.

Podman SDK를 사용하면 API 추상화와 기능 확장 가능성은 커지지만, 초기 범위에 비해 의존성과 학습 비용이 커질 수 있다.

## Decision

Podman 연동은 SDK 대신 `os/exec` 기반 CLI 호출로 구현한다.

구체적으로 `podman exec -i <container-name> radosgw-admin bucket stats
--bucket=<bucket-name>` 명령을 실행하고, stdout JSON을 파싱해 도메인
모델로 변환한다.

`github.com/containers/podman/v5` 같은 Podman API 라이브러리는 도입하지 않는다.

## Rationale

- 현재 범위에서는 컨테이너 내부 명령 실행만 필요하므로 CLI 호출만으로 충분하다.
- 외부 SDK를 추가하지 않아 의존성과 빌드 복잡도를 줄일 수 있다.
- 실제 운영 환경에서 사람이 수동으로 확인하는 명령과 코드 경로가 일치해 디버깅이 쉽다.
- 표준 라이브러리 `os/exec`만으로 구현 가능해 초기 구조를 가볍게 유지할 수 있다.

## Consequences

### Positive

- 의존성이 단순해지고 모듈 관리가 쉬워진다.
- Podman API 버전 변화에 직접 영향받는 면적이 줄어든다.
- 명령 조합과 stdout/stderr 처리 단위 테스트를 작성하기 쉽다.

### Negative

- 호출 가능한 기능이 CLI 계약에 직접 묶인다.
- 구조화된 API 대신 프로세스 실행과 문자열 인자 조합을 직접 다뤄야 한다.
- 장기적으로 Podman 고급 기능이 필요해지면 SDK 도입을 다시 검토해야 할 수 있다.

## Alternatives Considered

### Podman SDK 사용

확장 가능성과 구조화된 API라는 장점이 있지만, 현재 필요한 기능에 비해 구현 복잡도와 의존성 비용이 크다.

### Podman REST API 직접 사용

프로세스 실행을 줄일 수는 있지만, 초기 범위에서는 설정과 오류 처리 복잡도가 커진다. 이 프로젝트는 우선 단순한 명령 위임을 선택한다.
