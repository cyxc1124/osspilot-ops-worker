package lifecycle

import "fmt"

func requireAction(deleteDays, trashDays, versionDays, multipartDays *int) error {
	if deleteDays != nil || trashDays != nil || versionDays != nil || multipartDays != nil {
		return nil
	}
	return fmt.Errorf("At least one lifecycle action must be configured")
}

func validDays(n *int) error {
	if n == nil {
		return nil
	}
	if *n < 1 {
		return fmt.Errorf("lifecycle days must be at least 1")
	}
	return nil
}
