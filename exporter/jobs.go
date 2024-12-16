// SPDX-FileCopyrightText: 2023 Rivos Inc.
//
// SPDX-License-Identifier: Apache-2.0

package exporter

import (
	"bytes"
	"encoding/json"
	"fmt"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"golang.org/x/exp/slog"
)

type NodeResource struct {
	Mem float64 `json:"memory"`
}

type JobResource struct {
	AllocCpus  float64                  `json:"allocated_cpus"`
	AllocNodes map[string]*NodeResource `json:"allocated_nodes"`
}

type JobMetric struct {
	Account      string      `json:"account"`
	JobId        float64     `json:"job_id"`
	ArrayId      string      `json:"array_id"` // New label for job array
	EndTime      float64     `json:"end_time"`
	JobState     string      `json:"job_state"`
	Partition    string      `json:"partition"`
	UserName     string      `json:"user_name"`
	Features     string      `json:"features"`
	JobResources JobResource `json:"job_resources"`
}

type squeueResponse struct {
	Meta struct {
		SlurmVersion struct {
			Version struct {
				Major int `json:"major"`
				Micro int `json:"micro"`
				Minor int `json:"minor"`
			} `json:"version"`
			Release string `json:"release"`
		} `json:"Slurm"`
	} `json:"meta"`
	Errors []string    `json:"errors"`
	Jobs   []JobMetric `json:"jobs"`
}

type NAbleTime struct {
	time.Time
}

// UnmarshalJSON handles special cases for N/A or NONE timestamps.
func (nat *NAbleTime) UnmarshalJSON(data []byte) error {
	var tString string
	if err := json.Unmarshal(data, &tString); err != nil {
		return err
	}
	nullSet := map[string]struct{}{"N/A": {}, "NONE": {}}
	if _, ok := nullSet[tString]; ok {
		nat.Time = time.Time{}
		return nil
	}
	t, err := time.Parse("2006-01-02T15:04:05", tString)
	nat.Time = t
	return err
}

func totalAllocMem(resource *JobResource) float64 {
	var allocMem float64
	for _, node := range resource.AllocNodes {
		allocMem += node.Mem
	}
	return allocMem
}

type JobJsonFetcher struct {
	scraper    SlurmByteScraper
	cache      *AtomicThrottledCache[JobMetric]
	errCounter prometheus.Counter
}

func (jjf *JobJsonFetcher) fetch() ([]JobMetric, error) {
	data, err := jjf.scraper.FetchRawBytes()
	if err != nil {
		jjf.errCounter.Inc()
		return nil, err
	}
	var squeue squeueResponse
	err = json.Unmarshal(data, &squeue)
	if err != nil {
		slog.Error("Unmarshaling node metrics %q", err)
		return nil, err
	}
	for _, j := range squeue.Jobs {
		for _, resource := range j.JobResources.AllocNodes {
			resource.Mem *= 1e9
		}
	}
	return squeue.Jobs, nil
}

func (jjf *JobJsonFetcher) FetchMetrics() ([]JobMetric, error) {
	return jjf.cache.FetchOrThrottle(jjf.fetch)
}

func (jjf *JobJsonFetcher) ScrapeDuration() time.Duration {
	return jjf.scraper.Duration()
}

func (jjf *JobJsonFetcher) ScrapeError() prometheus.Counter {
	return jjf.errCounter
}

type JobCliFallbackFetcher struct {
	scraper    SlurmByteScraper
	cache      *AtomicThrottledCache[JobMetric]
	errCounter prometheus.Counter
}

func (jcf *JobCliFallbackFetcher) fetch() ([]JobMetric, error) {
	squeue, err := jcf.scraper.FetchRawBytes()
	if err != nil {
		return nil, err
	}

	jobMetrics := make([]JobMetric, 0)
	seenJobs := make(map[float64]bool) // Map to track unique JobIds

	// Clean and split input
	squeue = bytes.TrimSpace(squeue)
	squeue = bytes.Trim(squeue, "\n")
	if len(squeue) == 0 {
		// No jobs returned
		return nil, nil
	}

	for i, line := range bytes.Split(squeue, []byte("\n")) {
		var metric struct {
			Account   string    `json:"a"`
			JobId     float64   `json:"id"`
			ArrayId   string    `json:"array_id"` // Added array_id field
			EndTime   NAbleTime `json:"end_time"`
			JobState  string    `json:"state"`
			Partition string    `json:"p"`
			UserName  string    `json:"u"`
			Cpu       int64     `json:"cpu"`
			Mem       string    `json:"mem"`
		}
		if err := json.Unmarshal(line, &metric); err != nil {
			slog.Error(fmt.Sprintf("squeue fallback parse error: failed on line %d `%s`", i, line))
			jcf.errCounter.Inc()
			continue
		}

		// Skip duplicate JobIds
		if seenJobs[metric.JobId] {
			continue
		}
		seenJobs[metric.JobId] = true

		// Convert memory to float
		mem, err := MemToFloat(metric.Mem)
		if err != nil {
			slog.Error(fmt.Sprintf("squeue fallback parse error: failed on line %d `%s` with err `%q`", i, line, err))
			jcf.errCounter.Inc()
			continue
		}

		// Create JobMetric object
		openapiJobMetric := JobMetric{
			Account:   metric.Account,
			JobId:     metric.JobId,
			ArrayId:   metric.ArrayId,
			JobState:  metric.JobState,
			Partition: metric.Partition,
			UserName:  metric.UserName,
			EndTime:   float64(metric.EndTime.Unix()),
			JobResources: JobResource{
				AllocCpus:  float64(metric.Cpu),
				AllocNodes: map[string]*NodeResource{"0": {Mem: mem}},
			},
		}
		jobMetrics = append(jobMetrics, openapiJobMetric)
	}
	return jobMetrics, nil
}

func (jcf *JobCliFallbackFetcher) FetchMetrics() ([]JobMetric, error) {
	return jcf.cache.FetchOrThrottle(jcf.fetch)
}

func (jcf *JobCliFallbackFetcher) ScrapeDuration() time.Duration {
	return jcf.scraper.Duration()
}

func (jcf *JobCliFallbackFetcher) ScrapeError() prometheus.Counter {
	return jcf.errCounter
}

type JobsCollector struct {
	fetcher           SlurmMetricFetcher[JobMetric]
	fallback          bool
	jobStatus         *prometheus.Desc
	jobScrapeDuration *prometheus.Desc
	jobScrapeError    prometheus.Counter
}

func NewJobsController(config *Config) *JobsCollector {
	cliOpts := config.cliOpts
	fetcher := config.TraceConf.sharedFetcher
	return &JobsCollector{
		fetcher:           fetcher,
		fallback:          cliOpts.fallback,
		jobStatus:         prometheus.NewDesc("slurm_job_status", "Job status with ID, name, and array_id", []string{"job_id", "job_name", "state", "array_id"}, nil), // Added array_id as label
		jobScrapeDuration: prometheus.NewDesc("slurm_job_scrape_duration", fmt.Sprintf("how long the cmd %v took (ms)", cliOpts.squeue), nil, nil),
		jobScrapeError: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "slurm_job_scrape_error",
			Help: "slurm job scrape error",
		}),
	}
}

func (jc *JobsCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- jc.jobStatus
	ch <- jc.jobScrapeDuration
	ch <- jc.jobScrapeError.Desc()
}

func (jc *JobsCollector) Collect(ch chan<- prometheus.Metric) {
	defer func() {
		ch <- jc.jobScrapeError
	}()
	jobMetrics, err := jc.fetcher.FetchMetrics()
	ch <- prometheus.MustNewConstMetric(jc.jobScrapeDuration, prometheus.GaugeValue, float64(jc.fetcher.ScrapeDuration().Milliseconds()))
	if err != nil {
		slog.Error("fetcher failure %q", err)
		return
	}

	for _, job := range jobMetrics {
		// Send metrics for each job with array_id
		ch <- prometheus.MustNewConstMetric(
			jc.jobStatus,
			prometheus.GaugeValue,
			1,                              // Fixed value indicating job presence
			fmt.Sprintf("%.0f", job.JobId), // Convert JobId to string
			job.UserName,                   // User name (used as job name)
			job.JobState,                   // Job state
			job.ArrayId,                    // New array_id label
		)
	}
}

