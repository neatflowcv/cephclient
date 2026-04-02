package cli

import (
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/neatflowcv/cephclient/internal/pkg/domain"
)

var (
	errEmptySelection        = errors.New("selection is empty")
	errSelectionNotYesNo     = errors.New("confirmation must be yes or no")
	errSelectionOutOfRange   = errors.New("selection number out of range")
	errSelectionInvalidToken = errors.New("selection contains an invalid number")
	errSelectionDuplicate    = errors.New("selection contains a duplicate number")
)

type rmSupportSelection struct {
	entry  domain.BIEntry
	index  int
	number int
}

func parseSelectionNumbers(input string, maxNumber int) ([]int, error) {
	trimmed := strings.TrimSpace(input)
	if trimmed == "" {
		return nil, errEmptySelection
	}

	parts := strings.Split(trimmed, ",")
	numbers := make([]int, 0, len(parts))
	seen := make(map[int]struct{}, len(parts))

	for _, part := range parts {
		token := strings.TrimSpace(part)
		if token == "" {
			return nil, errSelectionInvalidToken
		}

		number, err := strconv.Atoi(token)
		if err != nil {
			return nil, fmt.Errorf("%w: %q", errSelectionInvalidToken, token)
		}

		if number < 1 || number > maxNumber {
			return nil, fmt.Errorf("%w: %d", errSelectionOutOfRange, number)
		}

		if _, ok := seen[number]; ok {
			return nil, fmt.Errorf("%w: %d", errSelectionDuplicate, number)
		}

		seen[number] = struct{}{}
		numbers = append(numbers, number)
	}

	return numbers, nil
}

func buildSelections(entries []domain.BIEntry, numbers []int) []rmSupportSelection {
	selections := make([]rmSupportSelection, 0, len(numbers))
	for _, number := range numbers {
		entryIndex := number - 1
		selections = append(selections, rmSupportSelection{
			entry:  entries[entryIndex],
			index:  entryIndex,
			number: number,
		})
	}

	return selections
}

func parseConfirmation(input string) (bool, error) {
	switch strings.TrimSpace(input) {
	case "yes":
		return true, nil
	case "no":
		return false, nil
	default:
		return false, errSelectionNotYesNo
	}
}
