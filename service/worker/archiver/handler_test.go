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
	"fmt"
	"math/rand"
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
	"go.uber.org/cadence"
	"go.uber.org/cadence/.gen/go/shared"
	"go.uber.org/cadence/testsuite"
	"go.uber.org/cadence/workflow"
	"go.uber.org/mock/gomock"

	"github.com/uber/cadence/common/log"
	"github.com/uber/cadence/common/metrics"
)

var (
	handlerTestMetrics *metrics.MockClient
	handlerTestLogger  *log.MockLogger
)

type handlerSuite struct {
	suite.Suite
	testsuite.WorkflowTestSuite
}

func TestHandlerSuite(t *testing.T) {
	suite.Run(t, new(handlerSuite))
}

func (s *handlerSuite) SetupSuite() {
	workflow.Register(handleHistoryRequestWorkflow)
	workflow.Register(handleVisibilityRequestWorkflow)
	workflow.Register(startAndFinishArchiverWorkflow)
}

func (s *handlerSuite) SetupTest() {
	handlerTestMetrics = metrics.NewMockClient(gomock.NewController(s.T()))
	handlerTestMetrics.EXPECT().StartTimer(gomock.Any(), gomock.Any()).Return(metrics.NopStopwatch()).AnyTimes()
	handlerTestLogger = log.NewMockLogger(gomock.NewController(s.T()))
	handlerTestLogger.EXPECT().WithTags(gomock.Any()).Return(handlerTestLogger).AnyTimes()
}

func (s *handlerSuite) TestHandleHistoryRequest_UploadFails_NonRetryableError() {
	handlerTestMetrics.EXPECT().IncCounter(metrics.ArchiverScope, metrics.ArchiverUploadFailedAllRetriesCount).Times(1)
	handlerTestMetrics.EXPECT().IncCounter(metrics.ArchiverScope, metrics.ArchiverDeleteSuccessCount).Times(1)
	handlerTestLogger.EXPECT().Error(gomock.Any(), gomock.Any()).Times(1)

	env := s.NewTestWorkflowEnvironment()
	env.OnActivity(uploadHistoryActivityFnName, mock.Anything, mock.Anything).Return(errors.New("some random error"))
	env.OnActivity(deleteHistoryActivityFnName, mock.Anything, mock.Anything).Return(nil)
	env.ExecuteWorkflow(handleHistoryRequestWorkflow, ArchiveRequest{})

	env.AssertExpectations(s.T())
	s.True(env.IsWorkflowCompleted())
	s.NoError(env.GetWorkflowError())
}

func (s *handlerSuite) TestHandleHistoryRequest_UploadFails_ExpireRetryTimeout() {
	handlerTestMetrics.EXPECT().IncCounter(metrics.ArchiverScope, metrics.ArchiverUploadFailedAllRetriesCount).Times(1)
	handlerTestMetrics.EXPECT().IncCounter(metrics.ArchiverScope, metrics.ArchiverDeleteSuccessCount).Times(1)
	handlerTestLogger.EXPECT().Error(gomock.Any(), gomock.Any()).Times(1)

	timeoutErr := workflow.NewTimeoutError(shared.TimeoutTypeStartToClose)
	env := s.NewTestWorkflowEnvironment()
	env.OnActivity(uploadHistoryActivityFnName, mock.Anything, mock.Anything).Return(timeoutErr)
	env.OnActivity(deleteHistoryActivityFnName, mock.Anything, mock.Anything).Return(nil)
	env.ExecuteWorkflow(handleHistoryRequestWorkflow, ArchiveRequest{})

	env.AssertExpectations(s.T())
	s.True(env.IsWorkflowCompleted())
	s.NoError(env.GetWorkflowError())
}

func (s *handlerSuite) TestHandleHistoryRequest_UploadSuccess() {
	handlerTestMetrics.EXPECT().IncCounter(metrics.ArchiverScope, metrics.ArchiverUploadSuccessCount).Times(1)
	handlerTestMetrics.EXPECT().IncCounter(metrics.ArchiverScope, metrics.ArchiverDeleteSuccessCount).Times(1)

	env := s.NewTestWorkflowEnvironment()
	env.OnActivity(uploadHistoryActivityFnName, mock.Anything, mock.Anything).Return(nil)
	env.OnActivity(deleteHistoryActivityFnName, mock.Anything, mock.Anything).Return(nil)
	env.ExecuteWorkflow(handleHistoryRequestWorkflow, ArchiveRequest{})

	env.AssertExpectations(s.T())
	s.True(env.IsWorkflowCompleted())
	s.NoError(env.GetWorkflowError())
}

func (s *handlerSuite) TestHandleHistoryRequest_DeleteFails_NonRetryableError() {
	handlerTestMetrics.EXPECT().IncCounter(metrics.ArchiverScope, metrics.ArchiverUploadSuccessCount).Times(1)
	handlerTestMetrics.EXPECT().IncCounter(metrics.ArchiverScope, metrics.ArchiverDeleteFailedAllRetriesCount).Times(1)
	handlerTestLogger.EXPECT().Error(gomock.Any(), gomock.Any()).Times(1)

	env := s.NewTestWorkflowEnvironment()
	env.OnActivity(uploadHistoryActivityFnName, mock.Anything, mock.Anything).Return(nil)
	env.OnActivity(deleteHistoryActivityFnName, mock.Anything, mock.Anything).Return(func(context.Context, ArchiveRequest) error {
		return cadence.NewCustomError(errDeleteNonRetriable.Error())
	})
	env.ExecuteWorkflow(handleHistoryRequestWorkflow, ArchiveRequest{})

	env.AssertExpectations(s.T())
	s.True(env.IsWorkflowCompleted())
	s.NoError(env.GetWorkflowError())
}

func (s *handlerSuite) TestHandleHistoryRequest_DeleteFailsThenSucceeds() {
	handlerTestMetrics.EXPECT().IncCounter(metrics.ArchiverScope, metrics.ArchiverUploadSuccessCount).Times(1)
	handlerTestMetrics.EXPECT().IncCounter(metrics.ArchiverScope, metrics.ArchiverDeleteSuccessCount).Times(1)

	env := s.NewTestWorkflowEnvironment()
	env.OnActivity(uploadHistoryActivityFnName, mock.Anything, mock.Anything).Return(nil)
	firstRun := true
	env.OnActivity(deleteHistoryActivityFnName, mock.Anything, mock.Anything).Return(func(context.Context, ArchiveRequest) error {
		if firstRun {
			firstRun = false
			return errors.New("some retryable error")
		}
		return nil
	})
	env.ExecuteWorkflow(handleHistoryRequestWorkflow, ArchiveRequest{})

	env.AssertExpectations(s.T())
	s.True(env.IsWorkflowCompleted())
	s.NoError(env.GetWorkflowError())
}

func (s *handlerSuite) TestHandleVisibilityRequest_Fail() {
	handlerTestMetrics.EXPECT().IncCounter(metrics.ArchiverScope, metrics.ArchiverHandleVisibilityFailedAllRetiresCount).Times(1)
	handlerTestLogger.EXPECT().Error(gomock.Any(), gomock.Any()).Times(1)

	env := s.NewTestWorkflowEnvironment()
	env.OnActivity(archiveVisibilityActivityFnName, mock.Anything, mock.Anything).Return(errors.New("some random error"))
	env.ExecuteWorkflow(handleVisibilityRequestWorkflow, ArchiveRequest{})

	env.AssertExpectations(s.T())
	s.True(env.IsWorkflowCompleted())
	s.NoError(env.GetWorkflowError())
}

func (s *handlerSuite) TestHandleVisibilityRequest_Success() {
	handlerTestMetrics.EXPECT().IncCounter(metrics.ArchiverScope, metrics.ArchiverHandleVisibilitySuccessCount).Times(1)

	env := s.NewTestWorkflowEnvironment()
	env.OnActivity(archiveVisibilityActivityFnName, mock.Anything, mock.Anything).Return(nil)
	env.ExecuteWorkflow(handleVisibilityRequestWorkflow, ArchiveRequest{})

	env.AssertExpectations(s.T())
	s.True(env.IsWorkflowCompleted())
	s.NoError(env.GetWorkflowError())
}

func (s *handlerSuite) TestRunArchiver() {
	numRequests := 1000
	concurrency := 10
	handlerTestMetrics.EXPECT().IncCounter(metrics.ArchiverScope, metrics.ArchiverUploadSuccessCount).Times(numRequests)
	handlerTestMetrics.EXPECT().IncCounter(metrics.ArchiverScope, metrics.ArchiverDeleteSuccessCount).Times(numRequests)
	handlerTestMetrics.EXPECT().IncCounter(metrics.ArchiverScope, metrics.ArchiverHandleVisibilitySuccessCount).Times(numRequests)
	handlerTestMetrics.EXPECT().IncCounter(metrics.ArchiverScope, metrics.ArchiverStartedCount).Times(1)
	handlerTestMetrics.EXPECT().IncCounter(metrics.ArchiverScope, metrics.ArchiverCoroutineStartedCount).Times(concurrency)
	handlerTestMetrics.EXPECT().IncCounter(metrics.ArchiverScope, metrics.ArchiverCoroutineStoppedCount).Times(concurrency)
	handlerTestMetrics.EXPECT().IncCounter(metrics.ArchiverScope, metrics.ArchiverStoppedCount).Times(1)

	env := s.NewTestWorkflowEnvironment()
	env.OnActivity(uploadHistoryActivityFnName, mock.Anything, mock.Anything).Return(nil)
	env.OnActivity(deleteHistoryActivityFnName, mock.Anything, mock.Anything).Return(nil)
	env.OnActivity(archiveVisibilityActivityFnName, mock.Anything, mock.Anything).Return(nil)
	env.ExecuteWorkflow(startAndFinishArchiverWorkflow, concurrency, numRequests)

	env.AssertExpectations(s.T())
	s.True(env.IsWorkflowCompleted())
	s.NoError(env.GetWorkflowError())
}

func handleHistoryRequestWorkflow(ctx workflow.Context, request ArchiveRequest) error {
	handler := NewHandler(ctx, handlerTestLogger, handlerTestMetrics, 0, nil).(*handler)
	handler.handleHistoryRequest(ctx, &request)
	return nil
}

func handleVisibilityRequestWorkflow(ctx workflow.Context, request ArchiveRequest) error {
	handler := NewHandler(ctx, handlerTestLogger, handlerTestMetrics, 0, nil).(*handler)
	handler.handleVisibilityRequest(ctx, &request)
	return nil
}

func startAndFinishArchiverWorkflow(ctx workflow.Context, concurrency int, numRequests int) error {
	requestCh := workflow.NewBufferedChannel(ctx, numRequests)
	handler := NewHandler(ctx, handlerTestLogger, handlerTestMetrics, concurrency, requestCh)
	handler.Start()
	sentHashes := make([]uint64, numRequests)
	workflow.Go(ctx, func(ctx workflow.Context) {
		for i := 0; i < numRequests; i++ {
			ar, hash := randomArchiveRequest()
			requestCh.Send(ctx, ar)
			sentHashes[i] = hash
		}
		requestCh.Close()
	})
	handledHashes := handler.Finished()
	if !hashesEqual(handledHashes, sentHashes) {
		return errors.New("handled hashes does not equal sent hashes")
	}
	return nil
}

func randomArchiveRequest() (ArchiveRequest, uint64) {
	ar := ArchiveRequest{
		DomainID:   fmt.Sprintf("%v", rand.Intn(1000)),
		WorkflowID: fmt.Sprintf("%v", rand.Intn(1000)),
		RunID:      fmt.Sprintf("%v", rand.Intn(1000)),
		Targets:    []ArchivalTarget{ArchiveTargetHistory, ArchiveTargetVisibility},
	}
	return ar, hash(ar)
}
