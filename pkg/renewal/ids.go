package renewal

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strconv"
	"time"
)

func WorkflowID(input RenewalInput) (string, error) {
	if err := ValidateRenewalInput(input); err != nil {
		return "", err
	}

	identity := input.PatientID + "\x00" + canonicalTime(input.CycleStart) + "\x00" + canonicalTime(input.CycleEnd)
	digest := sha256.Sum256([]byte(identity))
	return "renewal-" + hex.EncodeToString(digest[:]), nil
}

func AttemptID(workflowID string, attempt int) string {
	return workflowID + "/attempt/" + strconv.Itoa(attempt)
}

func ResolutionEventID(workflowID string) string {
	return workflowID + "/resolution"
}

func canonicalTime(value time.Time) string {
	return value.UTC().Format(time.RFC3339Nano)
}

func validateOpaqueID(name, value string) error {
	if value == "" {
		return fmt.Errorf("%s is required", name)
	}
	for _, char := range value {
		isASCIIWhitespaceOrControl := char <= ' '
		if isASCIIWhitespaceOrControl {
			return fmt.Errorf("%s contains whitespace or control characters", name)
		}
	}
	return nil
}
