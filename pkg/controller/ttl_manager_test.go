package controller

import (
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/sirupsen/logrus"

	coreapi "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/diff"
)

func TestResolveTtlStatus(t *testing.T) {
	var testCases = []struct {
		name           string
		ns             *coreapi.Namespace
		active         bool
		lastTransition time.Time
		podError       error
		expected       ttlStatus
		expectedErr    bool
		expectedRetry  bool
	}{
		{
			name: "no TTL configured, no delete-at present",
			ns: &coreapi.Namespace{
				ObjectMeta: metav1.ObjectMeta{
					Annotations: map[string]string{},
				},
			},
			expected: ttlStatus{
				hardTtlPresent:  false,
				softTtlPresent:  false,
				deleteAtPresent: false,
			},
		},
		{
			name: "only hard TTL configured, no delete-at present",
			ns: &coreapi.Namespace{
				ObjectMeta: metav1.ObjectMeta{
					CreationTimestamp: metav1.NewTime(time.Date(2000, time.January, 1, 1, 1, 1, 0, time.UTC)),
					Annotations: map[string]string{
						hardTTLAnnotation: time.Hour.String(),
					},
				},
			},
			expected: ttlStatus{
				hardTtlPresent:  true,
				hardDeleteAt:    time.Date(2000, time.January, 1, 2, 1, 1, 0, time.UTC),
				softTtlPresent:  false,
				deleteAtPresent: false,
			},
		},
		{
			name: "only delete-at present",
			ns: &coreapi.Namespace{
				ObjectMeta: metav1.ObjectMeta{
					Annotations: map[string]string{
						deleteAtAnnotation: time.Date(2000, time.February, 2, 2, 2, 2, 0, time.UTC).Format(time.RFC3339),
					},
				},
			},
			expected: ttlStatus{
				hardTtlPresent:  false,
				softTtlPresent:  false,
				deleteAtPresent: true,
				deleteAt:        time.Date(2000, time.February, 2, 2, 2, 2, 0, time.UTC),
			},
		},
		{
			name: "both hard and soft TTL configured, delete-at present, active ns",
			ns: &coreapi.Namespace{
				ObjectMeta: metav1.ObjectMeta{
					CreationTimestamp: metav1.NewTime(time.Date(2000, time.January, 1, 1, 1, 1, 0, time.UTC)),
					Annotations: map[string]string{
						hardTTLAnnotation:  time.Hour.String(),
						softTTLAnnotation:  time.Minute.String(),
						deleteAtAnnotation: time.Date(2000, time.February, 2, 2, 2, 2, 0, time.UTC).Format(time.RFC3339),
					},
				},
			},
			active: true,
			expected: ttlStatus{
				hardTtlPresent:  true,
				hardDeleteAt:    time.Date(2000, time.January, 1, 2, 1, 1, 0, time.UTC),
				softTtlPresent:  true,
				softDeleteAt:    time.Time{},
				active:          true,
				deleteAtPresent: true,
				deleteAt:        time.Date(2000, time.February, 2, 2, 2, 2, 0, time.UTC),
			},
		},
		{
			name: "only soft TTL configured, no delete-at present, active ns",
			ns: &coreapi.Namespace{
				ObjectMeta: metav1.ObjectMeta{
					Annotations: map[string]string{
						softTTLAnnotation: time.Minute.String(),
					},
				},
			},
			active: true,
			expected: ttlStatus{
				hardTtlPresent:  false,
				softTtlPresent:  true,
				softDeleteAt:    time.Time{},
				active:          true,
				deleteAtPresent: false,
			},
		},
		{
			name: "only soft TTL configured, no delete-at present, inactive ns",
			ns: &coreapi.Namespace{
				ObjectMeta: metav1.ObjectMeta{
					Annotations: map[string]string{
						softTTLAnnotation: time.Minute.String(),
					},
				},
			},
			active:         false,
			lastTransition: time.Date(2000, time.February, 3, 3, 3, 3, 0, time.UTC),
			expected: ttlStatus{
				hardTtlPresent:  false,
				softTtlPresent:  true,
				softDeleteAt:    time.Date(2000, time.February, 3, 3, 4, 3, 0, time.UTC),
				active:          false,
				deleteAtPresent: false,
			},
		},
		{
			name: "incorrectly formatted hard TTL",
			ns: &coreapi.Namespace{
				ObjectMeta: metav1.ObjectMeta{
					CreationTimestamp: metav1.NewTime(time.Date(2000, time.January, 1, 1, 1, 1, 0, time.UTC)),
					Annotations: map[string]string{
						hardTTLAnnotation:  "foo",
						softTTLAnnotation:  time.Minute.String(),
						deleteAtAnnotation: time.Date(2000, time.February, 2, 2, 2, 2, 0, time.UTC).Format(time.RFC3339),
					},
				},
			},
			expectedErr: true,
		},
		{
			name: "incorrectly formatted soft TTL",
			ns: &coreapi.Namespace{
				ObjectMeta: metav1.ObjectMeta{
					CreationTimestamp: metav1.NewTime(time.Date(2000, time.January, 1, 1, 1, 1, 0, time.UTC)),
					Annotations: map[string]string{
						hardTTLAnnotation:  time.Hour.String(),
						softTTLAnnotation:  "bar",
						deleteAtAnnotation: time.Date(2000, time.February, 2, 2, 2, 2, 0, time.UTC).Format(time.RFC3339),
					},
				},
			},
			expectedErr: true,
		},
		{
			name: "incorrectly formatted delete-at",
			ns: &coreapi.Namespace{
				ObjectMeta: metav1.ObjectMeta{
					CreationTimestamp: metav1.NewTime(time.Date(2000, time.January, 1, 1, 1, 1, 0, time.UTC)),
					Annotations: map[string]string{
						hardTTLAnnotation:  time.Hour.String(),
						softTTLAnnotation:  time.Minute.String(),
						deleteAtAnnotation: "baz",
					},
				},
			},
			expectedErr: true,
		},
		{
			name: "error processing pods never seen because we only have hard TTL",
			ns: &coreapi.Namespace{
				ObjectMeta: metav1.ObjectMeta{
					CreationTimestamp: metav1.NewTime(time.Date(2000, time.January, 1, 1, 1, 1, 0, time.UTC)),
					Annotations: map[string]string{
						hardTTLAnnotation: time.Hour.String(),
					},
				},
			},
			podError: fmt.Errorf("listing pods is on-demand so this should not happen"),
			expected: ttlStatus{
				hardTtlPresent:  true,
				hardDeleteAt:    time.Date(2000, time.January, 1, 2, 1, 1, 0, time.UTC),
				softTtlPresent:  false,
				deleteAtPresent: false,
			},
		},
		{
			name: "error processing pods seen because we have a soft ttl",
			ns: &coreapi.Namespace{
				ObjectMeta: metav1.ObjectMeta{
					Annotations: map[string]string{
						softTTLAnnotation: time.Minute.String(),
					},
				},
			},
			podError:      fmt.Errorf("listing pods is on-demand so this should happen"),
			expectedErr:   true,
			expectedRetry: true,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			processPods := func() (bool, time.Time, error) {
				return testCase.active, testCase.lastTransition, testCase.podError
			}
			log := logrus.New()
			log.Formatter = &logrus.JSONFormatter{}
			logger := log.WithField("test", testCase.name)
			actual, err, retry := resolveTtlStatus(testCase.ns, processPods, logger)
			if !testCase.expectedErr && err != nil {
				t.Errorf("%s: expected no error but got %v", testCase.name, err)
			}
			if testCase.expectedErr && err == nil {
				t.Errorf("%s: expected error but got none", testCase.name)
			}
			if testCase.expectedRetry != retry {
				t.Errorf("%s: expected retry %v but got %v", testCase.name, testCase.expectedRetry, retry)
			}
			if testCase.expectedErr {
				// if we have an error, status is not to be read
				return
			}
			// we don't care too much about the logger
			actual.logger = nil
			if !reflect.DeepEqual(actual, testCase.expected) {
				t.Errorf("%s: got incorrect TTL status:\n%s", testCase.name, diff.ObjectReflectDiff(testCase.expected, actual))
			}
		})
	}
}

func TestDetermineDeleteAt(t *testing.T) {
	var testCases = []struct {
		name             string
		status           ttlStatus
		expectedUpdate   bool
		expectedDelete   bool
		expectedDeleteAt time.Time
	}{
		{
			name: "no TTL configured, no delete-at present, should do nothing",
			status: ttlStatus{
				hardTtlPresent:  false,
				softTtlPresent:  false,
				deleteAtPresent: false,
			},
			expectedUpdate: false,
			expectedDelete: false,
		},
		{
			name: "no TTL configured, delete-at present, should delete",
			status: ttlStatus{
				hardTtlPresent:  false,
				softTtlPresent:  false,
				deleteAtPresent: true,
			},
			expectedDelete: true,
		},
		{
			name: "hard TTL configured, no delete-at present, should update",
			status: ttlStatus{
				hardTtlPresent:  true,
				hardDeleteAt:    time.Date(2000, time.January, 1, 1, 1, 1, 0, time.UTC),
				softTtlPresent:  false,
				deleteAtPresent: false,
			},
			expectedUpdate:   true,
			expectedDeleteAt: time.Date(2000, time.January, 1, 1, 1, 1, 0, time.UTC),
		},
		{
			name: "hard TTL configured, delete-at present but longer, should update",
			status: ttlStatus{
				hardTtlPresent:  true,
				hardDeleteAt:    time.Date(2000, time.January, 1, 1, 1, 1, 0, time.UTC),
				softTtlPresent:  false,
				deleteAtPresent: true,
				deleteAt:        time.Date(2001, time.January, 1, 1, 1, 1, 0, time.UTC),
			},
			expectedUpdate:   true,
			expectedDeleteAt: time.Date(2000, time.January, 1, 1, 1, 1, 0, time.UTC),
		},
		{
			name: "hard TTL configured, delete-at present but shorter, should do nothing",
			status: ttlStatus{
				hardTtlPresent:  true,
				hardDeleteAt:    time.Date(2000, time.January, 1, 1, 1, 1, 0, time.UTC),
				softTtlPresent:  false,
				deleteAtPresent: true,
				deleteAt:        time.Date(1999, time.January, 1, 1, 1, 1, 0, time.UTC),
			},
			expectedUpdate: false,
		},
		{
			name: "only soft TTL configured, namespace active, delete-at not present, do nothing",
			status: ttlStatus{
				hardTtlPresent:  false,
				softTtlPresent:  true,
				active:          true,
				deleteAtPresent: false,
			},
			expectedUpdate: false,
			expectedDelete: false,
		},
		{
			name: "soft and hard TTL configured, namespace active, delete-at not present, should update to hard TTL",
			status: ttlStatus{
				hardTtlPresent:  true,
				hardDeleteAt:    time.Date(2000, time.January, 1, 1, 1, 1, 0, time.UTC),
				softTtlPresent:  true,
				active:          true,
				deleteAtPresent: false,
			},
			expectedUpdate:   true,
			expectedDeleteAt: time.Date(2000, time.January, 1, 1, 1, 1, 0, time.UTC),
		},
		{
			name: "soft and hard TTL configured, namespace active, delete-at present and at hard TTL, should do nothing",
			status: ttlStatus{
				hardTtlPresent:  true,
				hardDeleteAt:    time.Date(2000, time.January, 1, 1, 1, 1, 0, time.UTC),
				softTtlPresent:  true,
				active:          true,
				deleteAtPresent: true,
				deleteAt:        time.Date(2000, time.January, 1, 1, 1, 1, 0, time.UTC),
			},
			expectedUpdate: false,
			expectedDelete: false,
		},
		{
			name: "soft and hard TTL configured, namespace active, delete-at present earlier than hard TTL, should update to hard TTL",
			status: ttlStatus{
				hardTtlPresent:  true,
				hardDeleteAt:    time.Date(2000, time.January, 1, 1, 1, 1, 0, time.UTC),
				softTtlPresent:  true,
				active:          true,
				deleteAtPresent: true,
				deleteAt:        time.Date(1999, time.January, 1, 1, 1, 1, 0, time.UTC),
			},
			expectedUpdate:   true,
			expectedDeleteAt: time.Date(2000, time.January, 1, 1, 1, 1, 0, time.UTC),
		},
		{
			name: "soft and hard TTL configured, namespace active, delete-at present later than hard TTL, should update to hard TTL",
			status: ttlStatus{
				hardTtlPresent:  true,
				hardDeleteAt:    time.Date(2000, time.January, 1, 1, 1, 1, 0, time.UTC),
				softTtlPresent:  true,
				active:          true,
				deleteAtPresent: true,
				deleteAt:        time.Date(2001, time.January, 1, 1, 1, 1, 0, time.UTC),
			},
			expectedUpdate:   true,
			expectedDeleteAt: time.Date(2000, time.January, 1, 1, 1, 1, 0, time.UTC),
		},
		{
			name: "only soft TTL configured, namespace inactive, delete-at not present, update to soft TTL",
			status: ttlStatus{
				hardTtlPresent:  false,
				softTtlPresent:  true,
				softDeleteAt:    time.Date(2000, time.January, 1, 1, 1, 1, 0, time.UTC),
				active:          false,
				deleteAtPresent: false,
			},
			expectedUpdate:   true,
			expectedDeleteAt: time.Date(2000, time.January, 1, 1, 1, 1, 0, time.UTC),
		},
		{
			name: "only soft TTL configured, namespace inactive, delete-at present and later, update to soft TTL",
			status: ttlStatus{
				hardTtlPresent:  false,
				softTtlPresent:  true,
				softDeleteAt:    time.Date(2000, time.January, 1, 1, 1, 1, 0, time.UTC),
				active:          false,
				deleteAtPresent: true,
				deleteAt:        time.Date(2001, time.January, 1, 1, 1, 1, 0, time.UTC),
			},
			expectedUpdate:   true,
			expectedDeleteAt: time.Date(2000, time.January, 1, 1, 1, 1, 0, time.UTC),
		},
		{
			name: "only soft TTL configured, namespace inactive, delete-at present and identical, do nothing",
			status: ttlStatus{
				hardTtlPresent:  false,
				softTtlPresent:  true,
				softDeleteAt:    time.Date(2000, time.January, 1, 1, 1, 1, 0, time.UTC),
				active:          false,
				deleteAtPresent: true,
				deleteAt:        time.Date(2000, time.January, 1, 1, 1, 1, 0, time.UTC),
			},
			expectedUpdate: false,
			expectedDelete: false,
		},
		{
			name: "only soft TTL configured, namespace inactive, delete-at present and earlier, update to soft TTL",
			status: ttlStatus{
				hardTtlPresent:  false,
				softTtlPresent:  true,
				softDeleteAt:    time.Date(2000, time.January, 1, 1, 1, 1, 0, time.UTC),
				active:          false,
				deleteAtPresent: true,
				deleteAt:        time.Date(1999, time.January, 1, 1, 1, 1, 0, time.UTC),
			},
			expectedUpdate:   true,
			expectedDeleteAt: time.Date(2000, time.January, 1, 1, 1, 1, 0, time.UTC),
		},
		{
			name: "both hard and soft TTL configured, namespace inactive, delete-at present and earlier than soft TTL at hard TTL, do nothing",
			status: ttlStatus{
				hardTtlPresent:  true,
				hardDeleteAt:    time.Date(1999, time.January, 1, 1, 1, 1, 0, time.UTC),
				softTtlPresent:  true,
				softDeleteAt:    time.Date(2000, time.January, 1, 1, 1, 1, 0, time.UTC),
				active:          false,
				deleteAtPresent: true,
				deleteAt:        time.Date(1999, time.January, 1, 1, 1, 1, 0, time.UTC),
			},
			expectedUpdate: false,
			expectedDelete: false,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			log := logrus.New()
			log.Formatter = &logrus.JSONFormatter{}
			logger := log.WithField("test", testCase.name)
			testCase.status.logger = logger
			actualUpdate, actualDelete, actualDeleteAt := determineDeleteAt(testCase.status)
			if actualUpdate != testCase.expectedUpdate {
				t.Errorf("%s: expected update %v, got %v", testCase.name, testCase.expectedUpdate, actualUpdate)
			}
			if actualDelete != testCase.expectedDelete {
				t.Errorf("%s: expected delete %v, got %v", testCase.name, testCase.expectedDelete, actualDelete)
			}
			if !actualDeleteAt.Equal(testCase.expectedDeleteAt) {
				t.Errorf("%s: expected %v, got %v", testCase.name, testCase.expectedDeleteAt, actualDeleteAt)
			}
		})
	}
}

func TestDigestPods(t *testing.T) {
	var testCases = []struct {
		name                       string
		pods                       []*coreapi.Pod
		expectedActive             bool
		expectedLastTransitionTime time.Time
	}{
		{
			name:                       "no pods",
			pods:                       []*coreapi.Pod{},
			expectedActive:             false,
			expectedLastTransitionTime: time.Time{},
		},
		{
			name: "pending pod",
			pods: []*coreapi.Pod{{
				Status: coreapi.PodStatus{
					Phase: coreapi.PodPending,
				},
			}},
			expectedActive:             true,
			expectedLastTransitionTime: time.Time{},
		},
		{
			name: "running pod",
			pods: []*coreapi.Pod{{
				Status: coreapi.PodStatus{
					Phase: coreapi.PodRunning,
				},
			}},
			expectedActive:             true,
			expectedLastTransitionTime: time.Time{},
		},
		{
			name: "terminated pod",
			pods: []*coreapi.Pod{{
				Status: coreapi.PodStatus{
					ContainerStatuses: []coreapi.ContainerStatus{{
						State: coreapi.ContainerState{
							Terminated: &coreapi.ContainerStateTerminated{
								FinishedAt: metav1.NewTime(time.Date(2000, time.January, 1, 1, 1, 1, 0, time.UTC)),
							},
						},
					}},
				},
			}},
			expectedActive:             false,
			expectedLastTransitionTime: time.Date(2000, time.January, 1, 1, 1, 1, 0, time.UTC),
		},
		{
			name: "terminated pods, expect latest termination time",
			pods: []*coreapi.Pod{{
				Status: coreapi.PodStatus{
					ContainerStatuses: []coreapi.ContainerStatus{{
						State: coreapi.ContainerState{
							Terminated: &coreapi.ContainerStateTerminated{
								FinishedAt: metav1.NewTime(time.Date(2001, time.January, 1, 1, 1, 1, 0, time.UTC)),
							},
						},
					}},
				},
			}, {
				Status: coreapi.PodStatus{
					ContainerStatuses: []coreapi.ContainerStatus{{
						State: coreapi.ContainerState{
							Terminated: &coreapi.ContainerStateTerminated{
								FinishedAt: metav1.NewTime(time.Date(2000, time.January, 1, 1, 1, 1, 0, time.UTC)),
							},
						},
					}},
				},
			}},
			expectedActive:             false,
			expectedLastTransitionTime: time.Date(2001, time.January, 1, 1, 1, 1, 0, time.UTC),
		},
	}

	for _, testCase := range testCases {
		active, lastTransitionTime := digestPods(testCase.pods)
		if active != testCase.expectedActive {
			t.Errorf("%s: expected namespace to be active: %v, but got %v", testCase.name, testCase.expectedActive, active)
		}
		if !lastTransitionTime.Equal(testCase.expectedLastTransitionTime) {
			t.Errorf("%s: expected namespace to be active: %v, but got %v", testCase.name, testCase.expectedLastTransitionTime, lastTransitionTime)
		}
	}
}
