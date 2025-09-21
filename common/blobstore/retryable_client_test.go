// The MIT License (MIT)

// Copyright (c) 2017-2020 Uber Technologies Inc.

// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.

package blobstore

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
	"go.uber.org/mock/gomock"

	"github.com/uber/cadence/common/backoff"
)

// RetryableClientTestSuite is the testify suite for retryable client tests
type RetryableClientTestSuite struct {
	suite.Suite
	mockCtrl   *gomock.Controller
	mockClient *MockClient
	policy     backoff.RetryPolicy
	client     Client
}

func (s *RetryableClientTestSuite) SetupTest() {
	s.mockCtrl = gomock.NewController(s.T())
	s.mockClient = NewMockClient(s.mockCtrl)
	s.policy = backoff.NewExponentialRetryPolicy(0)
	s.client = NewRetryableClient(s.mockClient, s.policy)
}

func (s *RetryableClientTestSuite) TearDownTest() {
	s.mockCtrl.Finish()
}

func (s *RetryableClientTestSuite) TestPut() {
	req := &PutRequest{}
	resp := &PutResponse{}
	s.mockClient.EXPECT().Put(gomock.Any(), req).Return(resp, nil).Times(1)

	result, err := s.client.Put(context.Background(), req)
	s.NoError(err)
	s.Equal(resp, result)
}

func (s *RetryableClientTestSuite) TestGet() {
	req := &GetRequest{}
	resp := &GetResponse{}
	s.mockClient.EXPECT().Get(gomock.Any(), req).Return(resp, nil).Times(1)

	result, err := s.client.Get(context.Background(), req)
	s.NoError(err)
	s.Equal(resp, result)
}

func (s *RetryableClientTestSuite) TestExists() {
	req := &ExistsRequest{}
	resp := &ExistsResponse{}
	s.mockClient.EXPECT().Exists(gomock.Any(), req).Return(resp, nil).Times(1)

	result, err := s.client.Exists(context.Background(), req)
	s.NoError(err)
	s.Equal(resp, result)
}

func (s *RetryableClientTestSuite) TestDelete() {
	req := &DeleteRequest{}
	resp := &DeleteResponse{}
	s.mockClient.EXPECT().Delete(gomock.Any(), req).Return(resp, nil).Times(1)

	result, err := s.client.Delete(context.Background(), req)
	s.NoError(err)
	s.Equal(resp, result)
}

func (s *RetryableClientTestSuite) TestRetryOnError() {
	s.policy = backoff.NewExponentialRetryPolicy(1)
	s.client = NewRetryableClient(s.mockClient, s.policy)

	req := &PutRequest{}
	resp := &PutResponse{}
	retryableError := errors.New("retryable error")

	s.mockClient.EXPECT().Put(gomock.Any(), req).Return(nil, retryableError).Times(1)
	s.mockClient.EXPECT().IsRetryableError(retryableError).Return(true).Times(1)
	s.mockClient.EXPECT().Put(gomock.Any(), req).Return(resp, nil).Times(1)

	result, err := s.client.Put(context.Background(), req)
	s.NoError(err, "Expected no error on successful retry")
	s.Equal(resp, result, "Expected the response to match")
}

func (s *RetryableClientTestSuite) TestNotRetryOnError() {
	req := &PutRequest{}
	nonRetryableError := errors.New("non-retryable error")

	s.mockClient.EXPECT().Put(gomock.Any(), req).Return(nil, nonRetryableError).Times(1)
	s.mockClient.EXPECT().IsRetryableError(nonRetryableError).Return(false).Times(1)

	result, err := s.client.Put(context.Background(), req)
	s.Error(err)
	s.Nil(result)
}

func (s *RetryableClientTestSuite) TestIsRetryableError() {
	retryableError := errors.New("retryable error")
	client := &retryableClient{
		client: s.mockClient,
		throttleRetry: backoff.NewThrottleRetry(
			backoff.WithRetryPolicy(backoff.NewExponentialRetryPolicy(0)),
			backoff.WithRetryableError(s.mockClient.IsRetryableError),
		),
	}

	s.mockClient.EXPECT().IsRetryableError(retryableError).Return(true).Times(1)

	isRetryable := client.IsRetryableError(retryableError)
	s.True(isRetryable, "Expected error to be retryable")
}

func TestRetryableClientTestSuite(t *testing.T) {
	suite.Run(t, new(RetryableClientTestSuite))
}
