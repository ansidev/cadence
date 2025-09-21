// Copyright (c) 2017 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package archiver

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
	"go.uber.org/cadence/testsuite"
	"go.uber.org/cadence/worker"
	"go.uber.org/mock/gomock"

	carchiver "github.com/uber/cadence/common/archiver"
	"github.com/uber/cadence/common/archiver/provider"
	"github.com/uber/cadence/common/dynamicconfig/dynamicproperties"
	"github.com/uber/cadence/common/log"
	"github.com/uber/cadence/common/log/testlogger"
	"github.com/uber/cadence/common/metrics"
	"github.com/uber/cadence/common/mocks"
	"github.com/uber/cadence/common/service"
)

const (
	testDomainID             = "test-domain-id"
	testDomainName           = "test-domain-name"
	testWorkflowID           = "test-workflow-id"
	testRunID                = "test-run-id"
	testNextEventID          = 1800
	testCloseFailoverVersion = 100
	testScheme               = "testScheme"
	testArchivalURI          = testScheme + "://history/archival"
)

var (
	testBranchToken = []byte{1, 2, 3}

	errPersistenceNonRetryable = errors.New("persistence non-retryable error")
)

type activitiesSuite struct {
	suite.Suite
	testsuite.WorkflowTestSuite

	controller         *gomock.Controller
	logger             log.Logger
	metricsClient      *metrics.MockClient
	metricsScope       *metrics.MockScope
	archiverProvider   *provider.MockArchiverProvider
	historyArchiver    *carchiver.HistoryArchiverMock
	visibilityArchiver *carchiver.VisibilityArchiverMock
}

func TestActivitiesSuite(t *testing.T) {
	suite.Run(t, new(activitiesSuite))
}

func (s *activitiesSuite) SetupTest() {
	s.controller = gomock.NewController(s.T())
	s.logger = testlogger.New(s.T())
	s.metricsClient = metrics.NewMockClient(s.controller)
	s.metricsScope = metrics.NewMockScope(s.controller)
	s.archiverProvider = provider.NewMockArchiverProvider(s.controller)
	s.historyArchiver = &carchiver.HistoryArchiverMock{}
	s.visibilityArchiver = &carchiver.VisibilityArchiverMock{}
	s.metricsScope.EXPECT().StartTimer(metrics.CadenceLatency).Return(metrics.NewTestStopwatch()).MaxTimes(1)
	s.metricsScope.EXPECT().RecordTimer(gomock.Any(), gomock.Any()).MaxTimes(1)
}

func (s *activitiesSuite) TearDownTest() {
	s.historyArchiver.AssertExpectations(s.T())
	s.visibilityArchiver.AssertExpectations(s.T())
}

func (s *activitiesSuite) TestUploadHistory_Fail_InvalidURI() {
	s.metricsClient.EXPECT().Scope(metrics.ArchiverUploadHistoryActivityScope, metrics.DomainTag(testDomainName)).Return(s.metricsScope).Times(1)
	s.metricsScope.EXPECT().IncCounter(metrics.ArchiverNonRetryableErrorCount).Times(1)
	container := &BootstrapContainer{
		Logger:        s.logger,
		MetricsClient: s.metricsClient,
		Config: &Config{
			AllowArchivingIncompleteHistory: dynamicproperties.GetBoolPropertyFn(false),
		},
	}
	env := s.NewTestActivityEnvironment()
	env.SetWorkerOptions(worker.Options{
		BackgroundActivityContext: context.WithValue(context.Background(), bootstrapContainerKey, container),
	})
	request := ArchiveRequest{
		DomainID:             testDomainID,
		DomainName:           testDomainName,
		WorkflowID:           testWorkflowID,
		RunID:                testRunID,
		BranchToken:          testBranchToken,
		NextEventID:          testNextEventID,
		CloseFailoverVersion: testCloseFailoverVersion,
		URI:                  "some invalid URI without scheme",
	}
	_, err := env.ExecuteActivity(uploadHistoryActivity, request)
	s.Equal(errUploadNonRetriable.Error(), err.Error())
}

func (s *activitiesSuite) TestUploadHistory_Fail_GetArchiverError() {
	s.metricsClient.EXPECT().Scope(metrics.ArchiverUploadHistoryActivityScope, metrics.DomainTag(testDomainName)).Return(s.metricsScope).Times(1)
	s.metricsScope.EXPECT().IncCounter(metrics.ArchiverNonRetryableErrorCount).Times(1)
	s.archiverProvider.EXPECT().GetHistoryArchiver(gomock.Any(), service.Worker).Return(nil, errors.New("failed to get archiver"))
	container := &BootstrapContainer{
		Logger:           s.logger,
		MetricsClient:    s.metricsClient,
		ArchiverProvider: s.archiverProvider,
		Config: &Config{
			AllowArchivingIncompleteHistory: dynamicproperties.GetBoolPropertyFn(false),
		},
	}
	env := s.NewTestActivityEnvironment()
	env.SetWorkerOptions(worker.Options{
		BackgroundActivityContext: context.WithValue(context.Background(), bootstrapContainerKey, container),
	})
	request := ArchiveRequest{
		DomainID:             testDomainID,
		DomainName:           testDomainName,
		WorkflowID:           testWorkflowID,
		RunID:                testRunID,
		BranchToken:          testBranchToken,
		NextEventID:          testNextEventID,
		CloseFailoverVersion: testCloseFailoverVersion,
		URI:                  testArchivalURI,
	}
	_, err := env.ExecuteActivity(uploadHistoryActivity, request)
	s.Equal(errUploadNonRetriable.Error(), err.Error())
}

func (s *activitiesSuite) TestUploadHistory_Fail_ArchiveNonRetriableError() {
	s.metricsClient.EXPECT().Scope(metrics.ArchiverUploadHistoryActivityScope, metrics.DomainTag(testDomainName)).Return(s.metricsScope).Times(1)
	s.metricsScope.EXPECT().IncCounter(metrics.ArchiverNonRetryableErrorCount).Times(1)
	s.historyArchiver.On("Archive", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(errUploadNonRetriable)
	s.archiverProvider.EXPECT().GetHistoryArchiver(gomock.Any(), service.Worker).Return(s.historyArchiver, nil)
	container := &BootstrapContainer{
		Logger:           s.logger,
		MetricsClient:    s.metricsClient,
		ArchiverProvider: s.archiverProvider,
		Config: &Config{
			AllowArchivingIncompleteHistory: dynamicproperties.GetBoolPropertyFn(false),
		},
	}
	env := s.NewTestActivityEnvironment()
	env.SetWorkerOptions(worker.Options{
		BackgroundActivityContext: context.WithValue(context.Background(), bootstrapContainerKey, container),
	})
	request := ArchiveRequest{
		DomainID:             testDomainID,
		DomainName:           testDomainName,
		WorkflowID:           testWorkflowID,
		RunID:                testRunID,
		BranchToken:          testBranchToken,
		NextEventID:          testNextEventID,
		CloseFailoverVersion: testCloseFailoverVersion,
		URI:                  testArchivalURI,
	}
	_, err := env.ExecuteActivity(uploadHistoryActivity, request)
	s.Equal(errUploadNonRetriable.Error(), err.Error())
}

func (s *activitiesSuite) TestUploadHistory_Fail_ArchiveRetriableError() {
	s.metricsClient.EXPECT().Scope(metrics.ArchiverUploadHistoryActivityScope, metrics.DomainTag(testDomainName)).Return(s.metricsScope).Times(1)
	testArchiveErr := errors.New("some transient error")
	s.historyArchiver.On("Archive", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(testArchiveErr)
	s.archiverProvider.EXPECT().GetHistoryArchiver(gomock.Any(), service.Worker).Return(s.historyArchiver, nil)
	container := &BootstrapContainer{
		Logger:           s.logger,
		MetricsClient:    s.metricsClient,
		ArchiverProvider: s.archiverProvider,
		Config: &Config{
			AllowArchivingIncompleteHistory: dynamicproperties.GetBoolPropertyFn(false),
		},
	}
	env := s.NewTestActivityEnvironment()
	env.SetWorkerOptions(worker.Options{
		BackgroundActivityContext: context.WithValue(context.Background(), bootstrapContainerKey, container),
	})
	request := ArchiveRequest{
		DomainID:             testDomainID,
		DomainName:           testDomainName,
		WorkflowID:           testWorkflowID,
		RunID:                testRunID,
		BranchToken:          testBranchToken,
		NextEventID:          testNextEventID,
		CloseFailoverVersion: testCloseFailoverVersion,
		URI:                  testArchivalURI,
	}
	_, err := env.ExecuteActivity(uploadHistoryActivity, request)
	s.Equal(testArchiveErr.Error(), err.Error())
}

func (s *activitiesSuite) TestUploadHistory_Success() {
	s.metricsClient.EXPECT().Scope(metrics.ArchiverUploadHistoryActivityScope, metrics.DomainTag(testDomainName)).Return(s.metricsScope).Times(1)
	s.historyArchiver.On("Archive", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.archiverProvider.EXPECT().GetHistoryArchiver(gomock.Any(), service.Worker).Return(s.historyArchiver, nil)
	container := &BootstrapContainer{
		Logger:           s.logger,
		MetricsClient:    s.metricsClient,
		ArchiverProvider: s.archiverProvider,
		Config: &Config{
			AllowArchivingIncompleteHistory: dynamicproperties.GetBoolPropertyFn(false),
		},
	}
	env := s.NewTestActivityEnvironment()
	env.SetWorkerOptions(worker.Options{
		BackgroundActivityContext: context.WithValue(context.Background(), bootstrapContainerKey, container),
	})
	request := ArchiveRequest{
		DomainID:             testDomainID,
		DomainName:           testDomainName,
		WorkflowID:           testWorkflowID,
		RunID:                testRunID,
		BranchToken:          testBranchToken,
		NextEventID:          testNextEventID,
		CloseFailoverVersion: testCloseFailoverVersion,
		URI:                  testArchivalURI,
	}
	_, err := env.ExecuteActivity(uploadHistoryActivity, request)
	s.NoError(err)
}

func (s *activitiesSuite) TestDeleteHistoryActivity_Fail_DeleteFromV2NonRetryableError() {
	s.metricsClient.EXPECT().Scope(metrics.ArchiverDeleteHistoryActivityScope, metrics.DomainTag(testDomainName)).Return(s.metricsScope).Times(1)
	s.metricsScope.EXPECT().IncCounter(metrics.ArchiverNonRetryableErrorCount).Times(1)
	mockHistoryV2Manager := &mocks.HistoryV2Manager{}
	mockHistoryV2Manager.On("DeleteHistoryBranch", mock.Anything, mock.Anything).Return(errPersistenceNonRetryable)
	container := &BootstrapContainer{
		Logger:           s.logger,
		MetricsClient:    s.metricsClient,
		HistoryV2Manager: mockHistoryV2Manager,
		Config: &Config{
			AllowArchivingIncompleteHistory: dynamicproperties.GetBoolPropertyFn(false),
		},
	}
	env := s.NewTestActivityEnvironment()
	env.SetWorkerOptions(worker.Options{
		BackgroundActivityContext: context.WithValue(context.Background(), bootstrapContainerKey, container),
	})
	request := ArchiveRequest{
		DomainID:             testDomainID,
		DomainName:           testDomainName,
		WorkflowID:           testWorkflowID,
		RunID:                testRunID,
		BranchToken:          testBranchToken,
		NextEventID:          testNextEventID,
		CloseFailoverVersion: testCloseFailoverVersion,
		URI:                  testArchivalURI,
	}
	_, err := env.ExecuteActivity(deleteHistoryActivity, request)
	s.Equal(errDeleteNonRetriable.Error(), err.Error())
}

func (s *activitiesSuite) TestArchiveVisibilityActivity_Fail_InvalidURI() {
	s.metricsClient.EXPECT().Scope(metrics.ArchiverArchiveVisibilityActivityScope, metrics.DomainTag(testDomainName)).Return(s.metricsScope).Times(1)
	s.metricsScope.EXPECT().IncCounter(metrics.ArchiverNonRetryableErrorCount).Times(1)
	container := &BootstrapContainer{
		Logger:        s.logger,
		MetricsClient: s.metricsClient,
		Config: &Config{
			AllowArchivingIncompleteHistory: dynamicproperties.GetBoolPropertyFn(false),
		},
	}
	env := s.NewTestActivityEnvironment()
	env.SetWorkerOptions(worker.Options{
		BackgroundActivityContext: context.WithValue(context.Background(), bootstrapContainerKey, container),
	})
	request := ArchiveRequest{
		DomainID:      testDomainID,
		DomainName:    testDomainName,
		WorkflowID:    testWorkflowID,
		RunID:         testRunID,
		VisibilityURI: "some invalid URI without scheme",
	}
	_, err := env.ExecuteActivity(archiveVisibilityActivity, request)
	s.Equal(errArchiveVisibilityNonRetriable.Error(), err.Error())
}

func (s *activitiesSuite) TestArchiveVisibilityActivity_Fail_GetArchiverError() {
	s.metricsClient.EXPECT().Scope(metrics.ArchiverArchiveVisibilityActivityScope, metrics.DomainTag(testDomainName)).Return(s.metricsScope).Times(1)
	s.metricsScope.EXPECT().IncCounter(metrics.ArchiverNonRetryableErrorCount).Times(1)
	s.archiverProvider.EXPECT().GetVisibilityArchiver(gomock.Any(), service.Worker).Return(nil, errors.New("failed to get archiver"))
	container := &BootstrapContainer{
		Logger:           s.logger,
		MetricsClient:    s.metricsClient,
		ArchiverProvider: s.archiverProvider,
		Config: &Config{
			AllowArchivingIncompleteHistory: dynamicproperties.GetBoolPropertyFn(false),
		},
	}
	env := s.NewTestActivityEnvironment()
	env.SetWorkerOptions(worker.Options{
		BackgroundActivityContext: context.WithValue(context.Background(), bootstrapContainerKey, container),
	})
	request := ArchiveRequest{
		DomainID:      testDomainID,
		DomainName:    testDomainName,
		WorkflowID:    testWorkflowID,
		RunID:         testRunID,
		VisibilityURI: testArchivalURI,
	}
	_, err := env.ExecuteActivity(archiveVisibilityActivity, request)
	s.Equal(errArchiveVisibilityNonRetriable.Error(), err.Error())
}

func (s *activitiesSuite) TestArchiveVisibilityActivity_Fail_ArchiveNonRetriableError() {
	s.metricsClient.EXPECT().Scope(metrics.ArchiverArchiveVisibilityActivityScope, metrics.DomainTag(testDomainName)).Return(s.metricsScope).Times(1)
	s.metricsScope.EXPECT().IncCounter(metrics.ArchiverNonRetryableErrorCount).Times(1)
	s.visibilityArchiver.On("Archive", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(errArchiveVisibilityNonRetriable)
	s.archiverProvider.EXPECT().GetVisibilityArchiver(gomock.Any(), service.Worker).Return(s.visibilityArchiver, nil)
	container := &BootstrapContainer{
		Logger:           s.logger,
		MetricsClient:    s.metricsClient,
		ArchiverProvider: s.archiverProvider,
		Config: &Config{
			AllowArchivingIncompleteHistory: dynamicproperties.GetBoolPropertyFn(false),
		},
	}
	env := s.NewTestActivityEnvironment()
	env.SetWorkerOptions(worker.Options{
		BackgroundActivityContext: context.WithValue(context.Background(), bootstrapContainerKey, container),
	})
	request := ArchiveRequest{
		DomainID:      testDomainID,
		DomainName:    testDomainName,
		WorkflowID:    testWorkflowID,
		RunID:         testRunID,
		VisibilityURI: testArchivalURI,
	}
	_, err := env.ExecuteActivity(archiveVisibilityActivity, request)
	s.Equal(errArchiveVisibilityNonRetriable.Error(), err.Error())
}

func (s *activitiesSuite) TestArchiveVisibilityActivity_Fail_ArchiveRetriableError() {
	s.metricsClient.EXPECT().Scope(metrics.ArchiverArchiveVisibilityActivityScope, metrics.DomainTag(testDomainName)).Return(s.metricsScope).Times(1)
	testArchiveErr := errors.New("some transient error")
	s.visibilityArchiver.On("Archive", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(testArchiveErr)
	s.archiverProvider.EXPECT().GetVisibilityArchiver(gomock.Any(), service.Worker).Return(s.visibilityArchiver, nil)
	container := &BootstrapContainer{
		Logger:           s.logger,
		MetricsClient:    s.metricsClient,
		ArchiverProvider: s.archiverProvider,
		Config: &Config{
			AllowArchivingIncompleteHistory: dynamicproperties.GetBoolPropertyFn(false),
		},
	}
	env := s.NewTestActivityEnvironment()
	env.SetWorkerOptions(worker.Options{
		BackgroundActivityContext: context.WithValue(context.Background(), bootstrapContainerKey, container),
	})
	request := ArchiveRequest{
		DomainID:      testDomainID,
		DomainName:    testDomainName,
		WorkflowID:    testWorkflowID,
		RunID:         testRunID,
		VisibilityURI: testArchivalURI,
	}
	_, err := env.ExecuteActivity(archiveVisibilityActivity, request)
	s.Equal(testArchiveErr.Error(), err.Error())
}

func (s *activitiesSuite) TestArchiveVisibilityActivity_Success() {
	s.metricsClient.EXPECT().Scope(metrics.ArchiverArchiveVisibilityActivityScope, metrics.DomainTag(testDomainName)).Return(s.metricsScope).Times(1)
	s.visibilityArchiver.On("Archive", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.archiverProvider.EXPECT().GetVisibilityArchiver(gomock.Any(), service.Worker).Return(s.visibilityArchiver, nil)
	container := &BootstrapContainer{
		Logger:           s.logger,
		MetricsClient:    s.metricsClient,
		ArchiverProvider: s.archiverProvider,
		Config: &Config{
			AllowArchivingIncompleteHistory: dynamicproperties.GetBoolPropertyFn(false),
		},
	}
	env := s.NewTestActivityEnvironment()
	env.SetWorkerOptions(worker.Options{
		BackgroundActivityContext: context.WithValue(context.Background(), bootstrapContainerKey, container),
	})
	request := ArchiveRequest{
		DomainID:      testDomainID,
		DomainName:    testDomainName,
		WorkflowID:    testWorkflowID,
		RunID:         testRunID,
		VisibilityURI: testArchivalURI,
	}
	_, err := env.ExecuteActivity(archiveVisibilityActivity, request)
	s.NoError(err)
}
