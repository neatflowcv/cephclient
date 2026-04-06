# ADR 0009: pending_map은 존재 여부만 표현하도록 단순화한다

- Status: Accepted
- Date: 2026-04-06

## Context

현재 도메인 모델의 `BIObjectEntry`는 `pendingMap []BIPendingMapEntry`를 보관한다.

`BIPendingMapEntry`는 `key`, `val`과 그 하위 값까지 유지하지만, 현재 도메인
레이어와 상위 호출 측은 이 상세 데이터를 해석하거나 의사결정에 사용하지 않는다.

이 구조를 유지하면 다음 비용이 생긴다.

- podman 응답의 `pending_map` 상세 값을 도메인 타입으로 계속 변환해야 한다.
- 도메인 객체가 실제로 쓰지 않는 세부 구조를 복사하고 노출하게 된다.
- 호출 측에서는 결국 pending 여부만 확인하면서도 API는 더 복잡하게 유지된다.

## Decision

이 프로젝트에서는 `pending_map`의 상세 구조를 도메인 모델로 올리지 않는다.

`BIObjectEntry`는 `pending_map`이 비어 있는지 여부만 표현하는 단순한 상태만
보관한다.

podman 응답 디코딩 단계에서는 `pending_map` 각 원소를 도메인 객체로 변환하지
않고, 항목이 하나 이상 존재하는지만 판단한다.

`pending_map` 상세 내용을 다시 외부로 노출하는 별도 API나 보존 구조는 추가하지
않는다.

## Rationale

- 현재 사용 방식에 맞춰 모델을 단순화하면 도메인 의도가 더 분명해진다.
- 불필요한 타입, 생성자, 복사 로직을 제거해 유지 비용을 줄일 수 있다.
- 응답 계층에서 존재 여부만 판별하면 데이터 경계가 더 명확해진다.
- 호출 측 API를 pending 여부 중심으로 정리하면 읽기와 사용이 쉬워진다.

## Consequences

### Positive

- `BIObjectEntry`와 관련 API가 더 작고 명확해진다.
- `BIPendingMapEntry` 및 관련 상세 구조를 제거할 수 있다.
- podman 응답을 도메인으로 옮기는 변환 로직이 단순해진다.
- 불필요한 복사와 상세 데이터 노출을 줄일 수 있다.

### Negative

- 앞으로 `pending_map` 상세 값을 실제로 사용해야 하면 다시 모델링이 필요하다.
- 기존에 상세 구조를 전제로 한 테스트나 호출 코드는 새 API에 맞게 정리해야 한다.

## Alternatives Considered

### 기존 `BIPendingMapEntry` 구조 유지

향후 활용 가능성을 위해 상세 데이터를 그대로 보존할 수는 있지만, 현재는 사용하지
않는 데이터를 계속 변환하고 노출하게 되어 비용이 더 크다.

### 상세 구조는 숨기고 원본 데이터만 별도로 보관

도메인 API는 단순해질 수 있지만, 결국 사용하지 않는 데이터를 다른 형태로 계속
유지해야 해서 복잡성 감소 효과가 작다.
