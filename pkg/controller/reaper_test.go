package controller

import (
	"testing"
	"time"

	coreapi "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/google/go-cmp/cmp"
)

func TestCreatedByCIOperatorAndExceedMaxAge(t *testing.T) {
	now := time.Now()
	time29DaysAgo := now.Add(-29 * 24 * time.Hour)
	time30DaysAgo := now.Add(-30 * 24 * time.Hour)
	time31DaysAgo := now.Add(-31 * 24 * time.Hour)

	testCases := []struct {
		name     string
		ns       *coreapi.Namespace
		oldest   time.Time
		expected bool
	}{
		{
			name: "delete if every condition is satisfied",
			ns: &coreapi.Namespace{
				ObjectMeta: metav1.ObjectMeta{
					Name:              "ci-op-hdzq1z7w",
					Labels:            map[string]string{"dptp.openshift.io/requester": "ci-operator"},
					CreationTimestamp: metav1.NewTime(time31DaysAgo),
				},
			},
			oldest:   time30DaysAgo,
			expected: true,
		},
		{
			name: "do not delete if not old enough",
			ns: &coreapi.Namespace{
				ObjectMeta: metav1.ObjectMeta{
					Name:              "ci-op-hdzq1z7w",
					Labels:            map[string]string{"dptp.openshift.io/requester": "ci-operator"},
					CreationTimestamp: metav1.NewTime(time29DaysAgo),
				},
			},
			oldest: time30DaysAgo,
		},
		{
			name: "do not delete if created by ci-secret-bootstrap",
			ns: &coreapi.Namespace{
				ObjectMeta: metav1.ObjectMeta{
					Name:              "ci-op-hdzq1z7w",
					Labels:            map[string]string{"dptp.openshift.io/requester": "ci-secret-bootstrap"},
					CreationTimestamp: metav1.NewTime(time31DaysAgo),
				},
			},
			oldest: time30DaysAgo,
		},
		{
			name: "do not delete if not created by ci-op",
			ns: &coreapi.Namespace{
				ObjectMeta: metav1.ObjectMeta{
					Name:              "ci-op-hdzq1z7w",
					CreationTimestamp: metav1.NewTime(time31DaysAgo),
				},
			},
			oldest: time30DaysAgo,
		},
		{
			name: "do not delete ci-op-jmp which would upset justin",
			ns: &coreapi.Namespace{
				ObjectMeta: metav1.ObjectMeta{
					Name:              "ci-op-jmp",
					Labels:            map[string]string{"dptp.openshift.io/requester": "ci-operator"},
					CreationTimestamp: metav1.NewTime(time31DaysAgo),
				},
			},
			oldest: time30DaysAgo,
		},
		{
			name: "do not delete default",
			ns: &coreapi.Namespace{
				ObjectMeta: metav1.ObjectMeta{
					Name:              "default",
					Labels:            map[string]string{"dptp.openshift.io/requester": "ci-operator"},
					CreationTimestamp: metav1.NewTime(time31DaysAgo),
				},
			},
			oldest: time30DaysAgo,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			actual := createdByCIOperatorAndExceedMaxAge(tc.ns, tc.oldest)
			if diff := cmp.Diff(tc.expected, actual); diff != "" {
				t.Errorf("%s: actual does not match expected, diff: %s", tc.name, diff)
			}
		})
	}
}
