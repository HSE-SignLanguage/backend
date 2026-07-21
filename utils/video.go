package utils

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"image"
	"image/jpeg"
	"math"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

const (
	ffprobeTimeout          = 15 * time.Second
	ffmpegTimeout           = 5 * time.Minute
	maxVideoDuration        = 2 * time.Minute
	maxVideoPixels          = 3840 * 2160
	maxExtractedVideoFrames = 960
	extractedFrameSize      = 448
)

type ffprobeStream struct {
	CodecType     string `json:"codec_type"`
	Width         int    `json:"width"`
	Height        int    `json:"height"`
	AvgFrameRate  string `json:"avg_frame_rate"`
	RealFrameRate string `json:"r_frame_rate"`
	Duration      string `json:"duration"`
}

type ffprobeResult struct {
	Streams []ffprobeStream `json:"streams"`
	Format  struct {
		Duration string `json:"duration"`
	} `json:"format"`
}

type VideoFrameExtractor struct {
	filePath  string
	frameRate float64
	duration  float64
	width     int
	height    int
}

func NewVideoFrameExtractor(filePath string) (*VideoFrameExtractor, error) {
	if _, err := os.Stat(filePath); os.IsNotExist(err) {
		return nil, fmt.Errorf("video file does not exist: %s", filePath)
	}

	info, err := getVideoInfo(filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to get video info: %w", err)
	}

	return &VideoFrameExtractor{
		filePath:  filePath,
		frameRate: info["fps"].(float64),
		duration:  info["duration"].(float64),
		width:     info["width"].(int),
		height:    info["height"].(int),
	}, nil
}

func getVideoInfo(filePath string) (map[string]interface{}, error) {
	ctx, cancel := context.WithTimeout(context.Background(), ffprobeTimeout)
	defer cancel()

	cmd := exec.CommandContext(ctx, "ffprobe",
		"-v", "quiet",
		"-print_format", "json",
		"-show_format",
		"-show_streams",
		filePath)

	output, err := cmd.Output()
	if err != nil {
		if contextErr := ctx.Err(); contextErr != nil {
			return nil, fmt.Errorf("ffprobe timed out: %w", contextErr)
		}
		return nil, fmt.Errorf("ffprobe failed: %w", err)
	}

	var probe ffprobeResult
	if err := json.Unmarshal(output, &probe); err != nil {
		return nil, fmt.Errorf("decode ffprobe output: %w", err)
	}

	var videoStream *ffprobeStream
	for index := range probe.Streams {
		if probe.Streams[index].CodecType == "video" {
			videoStream = &probe.Streams[index]
			break
		}
	}
	if videoStream == nil {
		return nil, fmt.Errorf("file contains no video stream")
	}

	fps, err := parseFrameRate(videoStream.AvgFrameRate)
	if err != nil {
		fps, err = parseFrameRate(videoStream.RealFrameRate)
		if err != nil {
			return nil, fmt.Errorf("invalid video frame rate: %w", err)
		}
	}

	durationText := videoStream.Duration
	if durationText == "" || durationText == "N/A" {
		durationText = probe.Format.Duration
	}
	duration, err := strconv.ParseFloat(durationText, 64)
	if err != nil || !isFinitePositive(duration) {
		return nil, fmt.Errorf("invalid video duration")
	}
	if duration > maxVideoDuration.Seconds() {
		return nil, fmt.Errorf("video duration exceeds %.0f seconds", maxVideoDuration.Seconds())
	}
	if videoStream.Width <= 0 || videoStream.Height <= 0 || videoStream.Width*videoStream.Height > maxVideoPixels {
		return nil, fmt.Errorf("video resolution is unsupported")
	}

	return map[string]interface{}{
		"fps":      fps,
		"duration": duration,
		"width":    videoStream.Width,
		"height":   videoStream.Height,
	}, nil
}

func parseFrameRate(value string) (float64, error) {
	parts := strings.SplitN(value, "/", 2)
	if len(parts) == 1 {
		fps, err := strconv.ParseFloat(parts[0], 64)
		if err != nil || !isFinitePositive(fps) || fps > 240 {
			return 0, fmt.Errorf("invalid frame rate %q", value)
		}
		return fps, nil
	}

	numerator, numeratorErr := strconv.ParseFloat(parts[0], 64)
	denominator, denominatorErr := strconv.ParseFloat(parts[1], 64)
	if numeratorErr != nil || denominatorErr != nil || !isFinitePositive(numerator) || !isFinitePositive(denominator) {
		return 0, fmt.Errorf("invalid frame rate %q", value)
	}

	fps := numerator / denominator
	if !isFinitePositive(fps) || fps > 240 {
		return 0, fmt.Errorf("invalid frame rate %q", value)
	}
	return fps, nil
}

func isFinitePositive(value float64) bool {
	return value > 0 && !math.IsNaN(value) && !math.IsInf(value, 0)
}

func (vfe *VideoFrameExtractor) ExtractAllFrames() ([][]byte, error) {
	return vfe.ExtractFramesWithInterval(1)
}

// EffectiveFrameInterval preserves the requested sampling interval unless it
// would exceed the bounded extraction budget. Longer or high-FPS videos are
// sampled less densely instead of being rejected despite satisfying the
// documented duration limit.
func (vfe *VideoFrameExtractor) EffectiveFrameInterval(requested int) int {
	if requested < 1 {
		requested = 1
	}

	minimum := int(math.Ceil(vfe.duration * vfe.frameRate / maxExtractedVideoFrames))
	if minimum > requested {
		return minimum
	}
	return requested
}

func (vfe *VideoFrameExtractor) ExtractFramesWithInterval(interval int) ([][]byte, error) {
	if interval < 1 {
		interval = 1
	}
	estimatedFrames := int(math.Ceil(vfe.duration * vfe.frameRate / float64(interval)))
	if estimatedFrames > maxExtractedVideoFrames {
		return nil, fmt.Errorf(
			"video would produce %d frames; increase interval to keep it below %d",
			estimatedFrames,
			maxExtractedVideoFrames,
		)
	}

	tempDir, err := os.MkdirTemp("", "video_frames_*")
	if err != nil {
		return nil, fmt.Errorf("failed to create temp directory: %w", err)
	}
	defer os.RemoveAll(tempDir)

	outputPattern := filepath.Join(tempDir, "frame_%04d.jpg")

	args := []string{
		"-nostdin",
		"-hide_banner",
		"-loglevel", "error",
		"-i", vfe.filePath,
		"-vf", fmt.Sprintf("select='not(mod(n\\,%d))',scale=%d:%d", interval, extractedFrameSize, extractedFrameSize),
		"-vsync", "vfr",
		"-frames:v", strconv.Itoa(maxExtractedVideoFrames + 1),
		"-q:v", "4",
		outputPattern,
	}

	ctx, cancel := context.WithTimeout(context.Background(), ffmpegTimeout)
	defer cancel()

	cmd := exec.CommandContext(ctx, "ffmpeg", args...)
	if err := cmd.Run(); err != nil {
		if contextErr := ctx.Err(); contextErr != nil {
			return nil, fmt.Errorf("ffmpeg timed out: %w", contextErr)
		}
		return nil, fmt.Errorf("ffmpeg failed: %w", err)
	}

	files, err := filepath.Glob(filepath.Join(tempDir, "frame_*.jpg"))
	if err != nil {
		return nil, fmt.Errorf("failed to list frame files: %w", err)
	}
	if len(files) > maxExtractedVideoFrames {
		return nil, fmt.Errorf("video produced more than %d frames", maxExtractedVideoFrames)
	}

	frames := make([][]byte, 0, len(files))
	for _, file := range files {
		frameData, err := os.ReadFile(file)
		if err != nil {
			continue
		}
		frames = append(frames, frameData)
	}

	return frames, nil
}

func (vfe *VideoFrameExtractor) GetVideoInfo() map[string]interface{} {
	estimatedFrames := int(vfe.duration * vfe.frameRate)
	return map[string]interface{}{
		"fps":              vfe.frameRate,
		"duration":         vfe.duration,
		"frame_width":      vfe.width,
		"frame_height":     vfe.height,
		"estimated_frames": estimatedFrames,
	}
}

func (vfe *VideoFrameExtractor) Close() error {
	return nil
}

func ConvertImageToJPEGBytes(img image.Image, quality int) ([]byte, error) {
	if quality <= 0 || quality > 100 {
		quality = 85
	}

	var buf bytes.Buffer
	if err := jpeg.Encode(&buf, img, &jpeg.Options{Quality: quality}); err != nil {
		return nil, fmt.Errorf("failed to encode image as JPEG: %w", err)
	}

	return buf.Bytes(), nil
}

func BatchFrames(frames [][]byte, batchSize int) [][][]byte {
	if batchSize <= 0 {
		batchSize = 32
	}

	batches := make([][][]byte, 0)
	for i := 0; i < len(frames); i += batchSize {
		end := i + batchSize
		if end > len(frames) {
			end = len(frames)
		}
		batch := append([][]byte(nil), frames[i:end]...)
		if len(batch) > 0 && len(batch) < batchSize {
			lastFrame := batch[len(batch)-1]
			for len(batch) < batchSize {
				batch = append(batch, lastFrame)
			}
		}
		batches = append(batches, batch)
	}

	return batches
}

// WindowFrames creates overlapping fixed-size inference windows. A single
// final partial window is padded with its last frame so the ML contract always
// receives exactly windowSize frames.
func WindowFrames(frames [][]byte, windowSize, stride int) [][][]byte {
	if len(frames) == 0 {
		return nil
	}
	if windowSize <= 0 {
		windowSize = 32
	}
	if stride <= 0 || stride > windowSize {
		stride = windowSize
	}

	windows := make([][][]byte, 0, (len(frames)+stride-1)/stride)
	for start := 0; start < len(frames); start += stride {
		end := start + windowSize
		if end > len(frames) {
			end = len(frames)
		}
		window := append([][]byte(nil), frames[start:end]...)
		if len(window) < windowSize {
			lastFrame := window[len(window)-1]
			for len(window) < windowSize {
				window = append(window, lastFrame)
			}
			windows = append(windows, window)
			break
		}
		windows = append(windows, window)
		if end == len(frames) {
			break
		}
	}
	return windows
}

func FramesToBase64(frames [][]byte) []string {
	base64Frames := make([]string, len(frames))
	for i, frame := range frames {
		base64Frames[i] = base64.StdEncoding.EncodeToString(frame)
	}
	return base64Frames
}

func ParseVideoMetadata(output string) map[string]string {
	metadata := make(map[string]string)
	lines := strings.Split(output, "\n")

	for _, line := range lines {
		parts := strings.SplitN(line, "=", 2)
		if len(parts) == 2 {
			key := strings.TrimSpace(parts[0])
			value := strings.TrimSpace(parts[1])
			metadata[key] = value
		}
	}

	return metadata
}

func GetVideoDuration(filePath string) (float64, error) {
	ctx, cancel := context.WithTimeout(context.Background(), ffprobeTimeout)
	defer cancel()

	cmd := exec.CommandContext(ctx, "ffprobe",
		"-v", "error",
		"-show_entries", "format=duration",
		"-of", "default=noprint_wrappers=1:nokey=1",
		filePath)

	output, err := cmd.Output()
	if err != nil {
		if contextErr := ctx.Err(); contextErr != nil {
			return 0, fmt.Errorf("ffprobe timed out: %w", contextErr)
		}
		return 0, fmt.Errorf("failed to get video duration: %w", err)
	}

	durationStr := strings.TrimSpace(string(output))
	duration, err := strconv.ParseFloat(durationStr, 64)
	if err != nil {
		return 0, fmt.Errorf("failed to parse duration: %w", err)
	}

	return duration, nil
}
