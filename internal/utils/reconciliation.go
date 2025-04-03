/*
Copyright 2024.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package utils

import (
	"os"
	"strconv"
	"time"
)

// Default reconciliation period in seconds
const DefaultReconciliationPeriodSeconds = 60

// GetReconciliationPeriod returns the reconciliation period from environment variable
// "RECONCILIATION_PERIOD_SECONDS" or defaults to 60 seconds if not set or invalid.
func GetReconciliationPeriod() time.Duration {
	envPeriod := os.Getenv("RECONCILIATION_PERIOD_SECONDS")
	if envPeriod != "" {
		if seconds, err := strconv.Atoi(envPeriod); err == nil && seconds > 0 {
			return time.Duration(seconds) * time.Second
		}
	}
	return DefaultReconciliationPeriodSeconds * time.Second
}
