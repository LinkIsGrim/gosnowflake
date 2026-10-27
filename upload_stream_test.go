package gosnowflake

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	sferrors "github.com/snowflakedb/gosnowflake/v2/internal/errors"
)

func TestBuildUploadStreamPutSQL(t *testing.T) {
	tail := " overwrite=true AUTO_COMPRESS=FALSE"
	assertEqualF(t, buildUploadStreamPutSQL("@~", "", true), "PUT "+uploadStreamDummyPutPath+" @~"+tail)
	assertEqualF(t, buildUploadStreamPutSQL("@mystage", "data/2024", true), "PUT "+uploadStreamDummyPutPath+" @mystage/data/2024"+tail)
	assertEqualF(t, buildUploadStreamPutSQL("@mystage", "/data", true), "PUT "+uploadStreamDummyPutPath+" @mystage/data"+tail)
	assertEqualF(t, buildUploadStreamPutSQL("@mystage", "data/", true), "PUT "+uploadStreamDummyPutPath+" @mystage/data/"+tail)
	assertEqualF(t, buildUploadStreamPutSQL("@mystage", "  ", true), "PUT "+uploadStreamDummyPutPath+" @mystage"+tail)
	assertEqualF(t, buildUploadStreamPutSQL("mystage", "dir", true), "PUT "+uploadStreamDummyPutPath+" @mystage/dir"+tail)
	assertEqualF(t, buildUploadStreamPutSQL("@mystage", "data/", false), "PUT "+uploadStreamDummyPutPath+" @mystage/data/ overwrite=false AUTO_COMPRESS=FALSE")
	quoted := buildUploadStreamPutSQL(`@"DB"."日本語".STAGE`, "dir/", true)
	assertEqualF(t, quoted, "PUT "+uploadStreamDummyPutPath+` '@"DB"."日本語".STAGE/dir/'`+tail)
}

func TestUploadStreamDestName(t *testing.T) {
	assertEqualF(t, uploadStreamDestName("hello.txt", true), "hello.txt.gz")
	assertEqualF(t, uploadStreamDestName("hello.txt.gz", true), "hello.txt.gz")
	assertEqualF(t, uploadStreamDestName("hello.txt.GZ", true), "hello.txt.GZ.gz")
	assertEqualF(t, uploadStreamDestName("hello.txt", false), "hello.txt")
}

func TestUploadStreamCompressDefault(t *testing.T) {
	assertTrueF(t, (UploadStreamConfig{}).compress(), "zero config compresses")
	assertTrueF(t, (UploadStreamConfig{Compression: CompressionGzip}).compress(), "explicit gzip compresses")
	assertFalseF(t, (UploadStreamConfig{Compression: CompressionNone}).compress(), "none stores the bytes as given")
	assertTrueF(t, (UploadStreamConfig{}).overwrite(), "zero config overwrites")
	assertFalseF(t, (UploadStreamConfig{Overwrite: ConfigBoolFalse}).overwrite(), "explicit false does not overwrite")
}

func TestUploadStreamRejectsEmptyArgs(t *testing.T) {
	sc := &snowflakeConn{}
	err := sc.UploadStream(context.Background(), "", "file.csv", stringsReader("x"))
	assertUploadStreamInvalidArg(t, err)

	err = sc.UploadStream(context.Background(), "@~", "  ", stringsReader("x"))
	assertUploadStreamInvalidArg(t, err)

	err = sc.UploadStream(context.Background(), "@~", "file.csv", nil)
	assertUploadStreamInvalidArg(t, err)

	err = sc.UploadStreamWithConfig(context.Background(), "@~", "file.csv", stringsReader("x"), UploadStreamConfig{
		MaxBuffer: -2,
	})
	assertUploadStreamInvalidArg(t, err)

	err = sc.UploadStreamWithConfig(context.Background(), "@~", "file.csv", stringsReader("x"), UploadStreamConfig{
		Compression: Compression(9),
	})
	assertUploadStreamInvalidArg(t, err)

	err = sc.UploadStream(context.Background(), "@~", "/", stringsReader("x"))
	assertUploadStreamInvalidArg(t, err)
	err = sc.UploadStream(context.Background(), "@~", "nested/memory.txt", stringsReader("x"))
	assertUploadStreamInvalidArg(t, err)
}

func TestUploadStreamLocalFSDoesNotRead(t *testing.T) {
	sc := &snowflakeConn{cfg: &Config{}}
	data := &execResponse{Data: execResponseData{
		Command:      string(uploadCommand),
		SrcLocations: []string{"/tmp/gosnowflake-upload-stream-missing"},
		StageInfo:    execResponseStageInfo{LocationType: "LOCAL_FS"},
	}}
	src := &readFlagReader{}
	err := sc.uploadStreamFromResponse(
		context.Background(),
		data,
		"PUT "+uploadStreamDummyPutPath+" @~ overwrite=true",
		"a.txt",
		src,
		UploadStreamConfig{},
	)
	assertNotNilF(t, err)
	var se *SnowflakeError
	assertErrorsAsF(t, err, &se)
	assertEqualF(t, se.Number, ErrUploadStreamLocalFS)
	assertFalseF(t, src.read, "local stage must be rejected before the source is read")
}

func TestSpoolStrategiesMatch(t *testing.T) {
	payload := []byte("upload-stream payload")
	for _, compress := range []bool{false, true} {
		root := t.TempDir()
		memory, err := spoolUploadStream(bytes.NewReader(payload), compress, MaxBufferUnbounded, root)
		assertNilF(t, err)
		disk, err := spoolUploadStream(bytes.NewReader(payload), compress, 1, root)
		assertNilF(t, err)
		spill, err := spoolUploadStream(bytes.NewReader(payload), compress, 1<<20, root)
		assertNilF(t, err)

		assertEqualF(t, memory.path, "")
		assertNotNilF(t, memory.buf, "memory strategy keeps the payload in a buffer")
		assertTrueF(t, disk.path != "", "disk strategy writes a temp file")
		assertTrueF(t, strings.HasPrefix(disk.path, root), "disk temp file uses Config.TmpDirPath")
		assertNilF(t, disk.buf, "disk strategy does not keep an object-sized buffer")
		assertEqualF(t, spill.path, "")
		assertNotNilF(t, spill.buf)

		memBytes, err := memory.bytes()
		assertNilF(t, err)
		diskBytes, err := disk.bytes()
		assertNilF(t, err)
		spillBytes, err := spill.bytes()
		assertNilF(t, err)
		assertBytesEqualE(t, diskBytes, memBytes)
		assertBytesEqualE(t, spillBytes, memBytes)
		assertEqualF(t, memory.digest, disk.digest)
		assertEqualF(t, spill.digest, memory.digest)
		assertEqualF(t, memory.digest, sha256Base64(memBytes))
		meta := newUploadStreamMetadata("hello.txt", compress, memory)
		assertEqualF(t, meta.sha256Digest, memory.digest)
		assertEqualF(t, meta.uploadSize, memory.size)
		assertEqualF(t, meta.dstFileName, uploadStreamDestName("hello.txt", compress))
		disk.cleanup()
		spill.cleanup()
		memory.cleanup()
	}
}

func TestSpoolSpillsAfterThreshold(t *testing.T) {
	payload := bytes.Repeat([]byte("a"), 8)
	root := t.TempDir()
	under, err := spoolUploadStream(bytes.NewReader(payload), false, 8, root)
	assertNilF(t, err)
	assertEqualF(t, under.path, "")
	assertEqualF(t, under.size, int64(8))
	got, err := under.bytes()
	assertNilF(t, err)
	assertBytesEqualE(t, got, payload)

	over, err := spoolUploadStream(bytes.NewReader(payload), false, 7, root)
	assertNilF(t, err)
	defer over.cleanup()
	assertTrueF(t, over.path != "", "payload one byte over the threshold spills")
	assertTrueF(t, strings.HasPrefix(over.path, root), "spilled temp file uses Config.TmpDirPath")
	assertNilF(t, over.buf)
	assertEqualF(t, over.size, int64(8))
	got, err = over.bytes()
	assertNilF(t, err)
	assertBytesEqualE(t, got, payload)
	assertEqualF(t, over.digest, sha256Base64(payload))
}

func TestSpoolDiskCleansUpOnReadError(t *testing.T) {
	root := t.TempDir()
	src := &bytesThenErr{b: bytes.Repeat([]byte("a"), 8), err: io.ErrUnexpectedEOF}
	_, err := spoolUploadStream(src, false, 1, root)
	assertNotNilF(t, err)
	entries, err := os.ReadDir(root)
	assertNilF(t, err)
	assertEqualF(t, len(entries), 0)
}

func TestSpoolHonorsCanceledContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	src := &readFlagReader{}
	_, err := spoolUploadStream(uploadStreamCtxReader{ctx: ctx, r: src}, false, MaxBufferUnbounded, "")
	assertErrIsF(t, err, context.Canceled, "canceled context stops the read")
	assertFalseF(t, src.read, "source is not read after cancel")

	root := t.TempDir()
	_, err = spoolUploadStream(uploadStreamCtxReader{ctx: ctx, r: src}, false, 1, root)
	assertErrIsF(t, err, context.Canceled, "canceled context stops a disk spool")
	entries, err := os.ReadDir(root)
	assertNilF(t, err)
	assertEqualF(t, len(entries), 0)
}

func TestUploadStreamPresignedCommand(t *testing.T) {
	sql := buildUploadStreamPutSQL("@mystage", "", true)
	assertEqualF(t, uploadStreamPresignedCommand(sql, "hello.txt.gz"), "PUT 'file:///tmp/hello.txt.gz' @mystage overwrite=true AUTO_COMPRESS=FALSE")
	assertEqualF(t, uploadStreamPresignedCommand(sql, "it's.txt"), "PUT 'file:///tmp/it''s.txt' @mystage overwrite=true AUTO_COMPRESS=FALSE")
}

func TestPresignedURLTargetsDest(t *testing.T) {
	match, err := url.Parse("https://storage.googleapis.com/bucket/prefix/hello.txt.gz?X-Goog-Signature=abc")
	assertNilF(t, err)
	assertTrueF(t, presignedURLTargetsDest(match, "hello.txt.gz"), "path suffix names the destination")

	nested, err := url.Parse("https://storage.googleapis.com/upload/storage/v1/b/bucket/o?name=prefix%2Fnested%2Fmemory.txt")
	assertNilF(t, err)
	assertTrueF(t, presignedURLTargetsDest(nested, "nested/memory.txt"), "query name names the destination")

	other, err := url.Parse("https://storage.googleapis.com/bucket/prefix/gosnowflake-upload-stream?X-Goog-Signature=abc")
	assertNilF(t, err)
	assertFalseF(t, presignedURLTargetsDest(other, "hello.txt.gz"), "dummy object name is not the destination")
	assertFalseF(t, presignedURLTargetsDest(nil, "hello.txt.gz"), "missing URL does not match")
}

func TestUploadStreamPresignedURLUsesDestName(t *testing.T) {
	posted := postUploadStreamPresignedQuery(t, "hello.txt.gz", "https://storage.googleapis.com/bucket/prefix/hello.txt.gz?X-Goog-Signature=abc")
	assertEqualF(t, posted, "PUT 'file:///tmp/hello.txt.gz' @mystage overwrite=true AUTO_COMPRESS=FALSE")
}

func TestUploadStreamPresignedURLRejectsMismatch(t *testing.T) {
	posted := postUploadStreamPresignedQuery(t, "hello.txt.gz", "https://storage.googleapis.com/bucket/prefix/other.txt?X-Goog-Signature=abc")
	assertEqualF(t, posted, "PUT 'file:///tmp/hello.txt.gz' @mystage overwrite=true AUTO_COMPRESS=FALSE")
}

func TestUploadStreamPresignedURLAllowsMissingURL(t *testing.T) {
	sfa := &snowflakeFileTransferAgent{
		ctx:               context.Background(),
		sc:                &snowflakeConn{cfg: &Config{}},
		data:              &execResponseData{},
		command:           buildUploadStreamPutSQL("@mystage", "", true),
		commandType:       uploadCommand,
		stageLocationType: gcsClient,
		stageInfo:         &execResponseStageInfo{Creds: execResponseCredentials{GcsAccessToken: "downscoped"}},
		streamUpload:      true,
		fileMetadata:      []*fileMetadata{{dstFileName: "hello.txt.gz"}},
	}
	assertNilF(t, sfa.updateFileMetadataWithPresignedURL(), "a downscoped token with no URL is accepted")
	assertNilF(t, sfa.fileMetadata[0].presignedURL)

	var posted string
	sfa.stageInfo = &execResponseStageInfo{}
	sfa.sc.rest = &snowflakeRestful{
		FuncPostQuery: func(_ context.Context, _ *snowflakeRestful, _ *url.Values, _ map[string]string, body []byte, _ time.Duration, _ UUID, _ *Config) (*execResponse, error) {
			var req execRequest
			assertNilF(t, json.Unmarshal(body, &req))
			posted = req.SQLText
			return &execResponse{Data: execResponseData{
				StageInfo: execResponseStageInfo{LocationType: "GCS"},
			}}, nil
		},
	}
	assertNilF(t, sfa.updateFileMetadataWithPresignedURL(), "an empty presigned URL is not a mismatch")
	assertStringContainsF(t, posted, "AUTO_COMPRESS=FALSE", "follow-up PUT keeps compression off")
	assertNilF(t, sfa.fileMetadata[0].presignedURL)
}

func TestUploadStreamGCSKeyJoinsPrefixAndDest(t *testing.T) {
	name := uploadStreamDestName("hello.txt", true)
	loc := (&snowflakeGcsClient{}).extractBucketNameAndPath("bucket/stage/data/2024")
	assertEqualF(t, loc.path+name, "stage/data/2024/hello.txt.gz")
	trailing := (&snowflakeGcsClient{}).extractBucketNameAndPath("bucket/stage/data/2024/")
	assertEqualF(t, trailing.path+name, "stage/data/2024/hello.txt.gz")
}

func TestGCSUploadStreamMemoryRetryKeepsBody(t *testing.T) {
	payload := []byte("retry-me")
	rec := &gcsBodyRecorder{status: http.StatusInternalServerError}
	presigned, err := url.Parse("https://storage.googleapis.com/bucket/prefix/hello.txt?X-Goog-Signature=abc")
	assertNilF(t, err)
	meta := &fileMetadata{
		srcStream:     bytes.NewBuffer(payload),
		presignedURL:  presigned,
		mockGcsClient: rec,
		dstFileName:   "hello.txt",
		sha256Digest:  "abc",
	}
	util := &snowflakeGcsClient{cfg: &Config{}}
	err = util.uploadFile(context.Background(), "", meta, 1, 0)
	assertNotNilF(t, err, "first attempt fails")
	err = util.uploadFile(context.Background(), "", meta, 1, 0)
	assertNotNilF(t, err, "second attempt fails")
	assertEqualF(t, len(rec.bodies), 2)
	assertBytesEqualE(t, rec.bodies[0], payload, "first PUT sends the buffer")
	assertBytesEqualE(t, rec.bodies[1], payload, "retry still has the buffer bytes")
}

func TestGCSUploadStreamCancelDuringPut(t *testing.T) {
	presigned, err := url.Parse("https://storage.googleapis.com/bucket/prefix/hello.txt")
	assertNilF(t, err)
	meta := &fileMetadata{
		srcStream:    bytes.NewBuffer([]byte("cancel-me")),
		presignedURL: presigned,
		mockGcsClient: &clientMock{DoFunc: func(req *http.Request) (*http.Response, error) {
			return nil, req.Context().Err()
		}},
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err = (&snowflakeGcsClient{cfg: &Config{}}).uploadFile(ctx, "", meta, 1, 0)
	assertErrIsF(t, err, context.Canceled, "cancel aborts the cloud PUT")
}

func TestUploadStreamEncryptRetryUsesOriginalBytes(t *testing.T) {
	payload := []byte("encrypt-me-please")
	meta := &fileMetadata{
		srcStream: bytes.NewBuffer(payload),
		encryptionMaterial: &snowflakeFileEncryption{
			QueryStageMasterKey: "abCdEFO0upIT36dAxGsa0w==",
			QueryID:             "01abc874-0406-1bf0-0000-53b10668e056",
			SMKID:               92019681909886,
		},
	}
	assertNilF(t, encryptDataIfRequired(meta, gcsClient))
	first := append([]byte(nil), meta.realSrcStream.Bytes()...)
	key, iv := meta.encryptMeta.key, meta.encryptMeta.iv
	assertTrueF(t, len(first) > len(payload), "ciphertext is larger than the payload")
	assertNilF(t, encryptDataIfRequired(meta, gcsClient))
	assertBytesEqualE(t, meta.realSrcStream.Bytes(), first, "retry reuses the ciphertext")
	assertEqualF(t, meta.encryptMeta.key, key, "retry keeps the encryption key")
	assertEqualF(t, meta.encryptMeta.iv, iv, "retry keeps the encryption IV")
	assertBytesEqualE(t, meta.srcStream.Bytes(), payload, "the spool buffer is not consumed")
}

func TestUploadStreamEncryptRetryKeepsCiphertext(t *testing.T) {
	payload := []byte("encrypt-me-please")
	t.Run("memory", func(t *testing.T) {
		buf := bytes.NewBuffer(payload)
		meta := newEncryptRetryMetadata(t, buf, "")
		meta.fileStream = bytes.NewReader(buf.Bytes())
		first, second := uploadEncryptRenewTwice(t, meta)
		assertBytesEqualE(t, second, first, "the second attempt uploads the same ciphertext")
		assertBytesEqualE(t, meta.srcStream.Bytes(), payload, "the spool buffer is not consumed")
	})
	t.Run("disk", func(t *testing.T) {
		root := t.TempDir()
		payloadFile, err := os.CreateTemp(root, "payload-")
		assertNilF(t, err)
		_, err = payloadFile.Write(payload)
		assertNilF(t, err)
		assertNilF(t, payloadFile.Close())
		meta := newEncryptRetryMetadata(t, nil, payloadFile.Name())
		meta.encTempRoot = root
		first, second := uploadEncryptRenewTwice(t, meta)
		assertBytesEqualE(t, second, first, "the second attempt uploads the same ciphertext")
		_, err = os.Stat(meta.realSrcFileName)
		assertNilF(t, err, "ciphertext file is kept for the retry")
		assertNotEqualF(t, meta.realSrcFileName, payloadFile.Name(), "ciphertext is a different file from the spill")
		meta.mockGcsClient = gcsStatusClient(http.StatusInternalServerError, nil)
		_, err = meta.sfa.uploadOneFile(meta)
		assertNotNilF(t, err, "the upload still fails")
		encDir := meta.tmpDir
		assertNotEqualF(t, encDir, "", "encryption created a temp directory")
		meta.mockGcsClient = gcsStatusClient(http.StatusInternalServerError, nil)
		_, err = meta.sfa.uploadOneFile(meta)
		assertNotNilF(t, err, "the upload still fails")
		_, err = os.Stat(encDir)
		assertNotNilF(t, err, "encryption temp dir is removed when the upload finishes")
		_, err = os.Stat(payloadFile.Name())
		assertNilF(t, err, "the spill file remains")
	})
}

func newEncryptRetryMetadata(t *testing.T, src *bytes.Buffer, srcFile string) *fileMetadata {
	t.Helper()
	meta := &fileMetadata{
		srcStream:      src,
		srcFileName:    srcFile,
		dstFileName:    "hello.txt",
		sha256Digest:   "abc",
		overwrite:      true,
		noSleepingTime: true,
		parallel:       1,
		client:         "tok",
		stageInfo:      &execResponseStageInfo{LocationType: "GCS", Location: "bucket/prefix"},
		options:        &SnowflakeFileTransferOptions{MultiPartThreshold: multiPartThreshold},
		encryptionMaterial: &snowflakeFileEncryption{
			QueryStageMasterKey: "abCdEFO0upIT36dAxGsa0w==",
			QueryID:             "01abc874-0406-1bf0-0000-53b10668e056",
			SMKID:               92019681909886,
		},
	}
	meta.sfa = &snowflakeFileTransferAgent{
		ctx:               context.Background(),
		sc:                &snowflakeConn{cfg: &Config{}},
		stageLocationType: gcsClient,
	}
	return meta
}

func uploadEncryptRenewTwice(t *testing.T, meta *fileMetadata) ([]byte, []byte) {
	t.Helper()
	var bodies [][]byte
	meta.mockGcsClient = gcsStatusClient(http.StatusUnauthorized, func(body []byte) {
		bodies = append(bodies, body)
	})
	_, err := meta.sfa.uploadOneFile(meta)
	assertNilF(t, err, "token renewal returns to the agent")
	assertEqualF(t, meta.resStatus, renewToken, "the first attempt asks for a new token")
	assertNotNilF(t, meta.encryptMeta, "the first attempt encrypts the payload")
	key, iv := meta.encryptMeta.key, meta.encryptMeta.iv
	_, err = meta.sfa.uploadOneFile(meta)
	assertNilF(t, err, "the retry returns to the agent")
	assertEqualF(t, meta.resStatus, renewToken, "the retry asks for a new token")
	assertEqualF(t, meta.encryptMeta.key, key, "the retry keeps the encryption key")
	assertEqualF(t, meta.encryptMeta.iv, iv, "the retry keeps the encryption IV")
	assertEqualF(t, len(bodies), 2, "each attempt uploads once")
	return bodies[0], bodies[1]
}

func gcsStatusClient(status int, onBody func([]byte)) *clientMock {
	return &clientMock{DoFunc: func(req *http.Request) (*http.Response, error) {
		if onBody != nil && req.Body != nil {
			body, err := io.ReadAll(req.Body)
			if err != nil {
				return nil, err
			}
			onBody(body)
		}
		return &http.Response{
			StatusCode: status,
			Status:     http.StatusText(status),
			Body:       io.NopCloser(bytes.NewReader(nil)),
			Header:     make(http.Header),
		}, nil
	}}
}

func TestUploadStreamRenewalKeepsSkipFileTransfer(t *testing.T) {
	presignedCalls := 0
	var sawSkip bool
	attempts := 0
	meta := &fileMetadata{
		srcStream:      bytes.NewBuffer([]byte("renew-me")),
		fileStream:     bytes.NewReader([]byte("renew-me")),
		dstFileName:    "renew.txt",
		sha256Digest:   "abc",
		overwrite:      true,
		noSleepingTime: true,
		srcFileSize:    8,
		stageInfo:      &execResponseStageInfo{LocationType: "GCS", Location: "bucket/prefix", Creds: execResponseCredentials{GcsAccessToken: "tok"}},
		mockGcsClient: &clientMock{DoFunc: func(req *http.Request) (*http.Response, error) {
			attempts++
			status := http.StatusUnauthorized
			if attempts > 1 {
				status = http.StatusInternalServerError
			}
			return &http.Response{
				StatusCode: status,
				Status:     http.StatusText(status),
				Body:       io.NopCloser(bytes.NewReader(nil)),
				Header:     make(http.Header),
			}, nil
		}},
	}
	sfa := &snowflakeFileTransferAgent{
		ctx:               context.Background(),
		streamUpload:      true,
		command:           buildUploadStreamPutSQL("@mystage", "", true),
		commandType:       uploadCommand,
		stageLocationType: gcsClient,
		stageInfo:         meta.stageInfo,
		parallel:          1,
		overwrite:         true,
		options:           &SnowflakeFileTransferOptions{MultiPartThreshold: multiPartThreshold},
		data:              &execResponseData{},
		sc: &snowflakeConn{
			cfg: &Config{},
			rest: &snowflakeRestful{
				FuncPostQuery: func(ctx context.Context, _ *snowflakeRestful, _ *url.Values, _ map[string]string, _ []byte, _ time.Duration, _ UUID, _ *Config) (*execResponse, error) {
					presignedCalls++
					sawSkip = isSkipFileTransfer(ctx)
					return &execResponse{Success: true, Data: execResponseData{
						StageInfo: *meta.stageInfo,
					}}, nil
				},
			},
		},
	}
	err := sfa.uploadStreamObject(meta)
	assertNotNilF(t, err, "the retried upload still fails")
	assertEqualF(t, presignedCalls, 1, "token renewal execs once")
	assertTrueF(t, sawSkip, "token renewal keeps skip-file-transfer")
	assertTrueF(t, attempts > 1, "upload is retried after renewal")
}

func TestUploadStreamSkippedBeforeRead(t *testing.T) {
	rt := &statusRoundTripper{status: http.StatusOK}
	src := &readFlagReader{}
	sc := &snowflakeConn{cfg: &Config{Transporter: rt}}
	data := &execResponse{Data: execResponseData{
		Command:      string(uploadCommand),
		SrcLocations: []string{"/tmp/gosnowflake-upload-stream"},
		StageInfo: execResponseStageInfo{
			LocationType: "GCS",
			Location:     "bucket/prefix",
			Creds:        execResponseCredentials{GcsAccessToken: "tok"},
		},
	}}
	err := sc.uploadStreamFromResponse(context.Background(), data, buildUploadStreamPutSQL("@mystage", "", false), "hello.txt", src, UploadStreamConfig{
		Overwrite: ConfigBoolFalse,
	})
	var se *SnowflakeError
	assertErrorsAsF(t, err, &se)
	assertEqualF(t, se.Number, ErrUploadStreamSkipped)
	assertFalseF(t, src.read, "an existing object is rejected before the source is read")
	assertTrueF(t, rt.calls > 0, "existence is checked with HEAD")
}

func TestUploadStreamRetryLoopReturnsCancel(t *testing.T) {
	presigned, err := url.Parse("https://storage.googleapis.com/bucket/prefix/hello.txt")
	assertNilF(t, err)
	calls := 0
	meta := &fileMetadata{
		realSrcFileName: "hello.txt",
		dstFileName:     "hello.txt",
		overwrite:       true,
		noSleepingTime:  true,
		presignedURL:    presigned,
		srcStream:       bytes.NewBuffer([]byte("x")),
		stageInfo:       &execResponseStageInfo{LocationType: "GCS", Location: "bucket/prefix"},
		options:         &SnowflakeFileTransferOptions{MultiPartThreshold: multiPartThreshold},
		sfa:             &snowflakeFileTransferAgent{sc: &snowflakeConn{cfg: &Config{}}},
		mockGcsClient: &clientMock{DoFunc: func(req *http.Request) (*http.Response, error) {
			calls++
			return nil, context.Canceled
		}},
	}
	util := &remoteStorageUtil{cfg: &Config{}}
	err = util.uploadOneFile(context.Background(), meta)
	assertErrIsF(t, err, context.Canceled, "a canceled PUT is returned from the retry loop")
	assertEqualF(t, calls, 1, "cancel does not start another attempt")

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err = util.uploadOneFile(ctx, meta)
	assertErrIsF(t, err, context.Canceled, "cancel before an attempt returns immediately")
	assertEqualF(t, calls, 1, "a canceled context does not call the cloud")
}

func TestUploadStreamRetrySleepHonorsCancel(t *testing.T) {
	presigned, err := url.Parse("https://storage.googleapis.com/bucket/prefix/hello.txt")
	assertNilF(t, err)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	meta := &fileMetadata{
		realSrcFileName: "hello.txt",
		dstFileName:     "hello.txt",
		overwrite:       true,
		presignedURL:    presigned,
		srcStream:       bytes.NewBuffer([]byte("x")),
		stageInfo:       &execResponseStageInfo{LocationType: "GCS", Location: "bucket/prefix"},
		options:         &SnowflakeFileTransferOptions{MultiPartThreshold: multiPartThreshold},
		sfa:             &snowflakeFileTransferAgent{sc: &snowflakeConn{cfg: &Config{}}},
		mockGcsClient: &clientMock{DoFunc: func(req *http.Request) (*http.Response, error) {
			cancel()
			return &http.Response{
				StatusCode: http.StatusInternalServerError,
				Status:     "500 Internal Server Error",
				Body:       io.NopCloser(bytes.NewReader(nil)),
				Header:     make(http.Header),
			}, nil
		}},
	}
	started := time.Now()
	err = (&remoteStorageUtil{cfg: &Config{}}).uploadOneFile(ctx, meta)
	assertErrIsF(t, err, context.Canceled, "cancel stops the retry sleep")
	assertTrueF(t, time.Since(started) < time.Second, "backoff sleep does not run after cancel")
}

func TestUploadStreamEncryptionTempUsesSpillDir(t *testing.T) {
	root := t.TempDir()
	payload, err := os.CreateTemp(root, "payload-")
	assertNilF(t, err)
	_, err = payload.Write([]byte("disk"))
	assertNilF(t, err)
	assertNilF(t, payload.Close())
	presigned, err := url.Parse("https://storage.googleapis.com/bucket/prefix/disk.txt")
	assertNilF(t, err)
	var encDir string
	meta := &fileMetadata{
		srcFileName:    payload.Name(),
		dstFileName:    "disk.txt",
		encTempRoot:    root,
		overwrite:      true,
		noSleepingTime: true,
		sha256Digest:   "abc",
		presignedURL:   presigned,
		stageInfo:      &execResponseStageInfo{LocationType: "GCS", Location: "bucket/prefix"},
		options:        &SnowflakeFileTransferOptions{MultiPartThreshold: multiPartThreshold},
	}
	meta.mockGcsClient = &clientMock{DoFunc: func(req *http.Request) (*http.Response, error) {
		encDir = meta.tmpDir
		return &http.Response{
			StatusCode: http.StatusInternalServerError,
			Status:     "500 Internal Server Error",
			Body:       io.NopCloser(bytes.NewReader(nil)),
			Header:     make(http.Header),
		}, nil
	}}
	meta.sfa = &snowflakeFileTransferAgent{
		ctx:               context.Background(),
		sc:                &snowflakeConn{cfg: &Config{}},
		stageLocationType: gcsClient,
	}
	_, err = meta.sfa.uploadOneFile(meta)
	assertNotNilF(t, err)
	assertTrueF(t, strings.HasPrefix(encDir, root), "encryption temp dir is under the spill directory")
	assertNotEqualF(t, encDir, root, "encryption uses its own directory inside the spill directory")
}

func TestUploadStreamRequiresUploadedStatus(t *testing.T) {
	sfa := &snowflakeFileTransferAgent{data: &execResponseData{QueryID: "q"}}
	sfa.results = []*fileMetadata{{resStatus: errStatus, lastError: io.ErrUnexpectedEOF}}
	err := sfa.uploadedStreamResult()
	var se *SnowflakeError
	assertErrorsAsF(t, err, &se)
	assertEqualF(t, se.Number, ErrFailedToUploadToStage)
	assertEqualF(t, se.Message, io.ErrUnexpectedEOF.Error())

	sfa.results = []*fileMetadata{{resStatus: uploaded}}
	assertNilF(t, sfa.uploadedStreamResult())

	sfa.results = []*fileMetadata{{resStatus: skipped}}
	err = sfa.uploadedStreamResult()
	assertErrorsAsF(t, err, &se)
	assertEqualF(t, se.Number, ErrUploadStreamSkipped)
	assertEqualF(t, se.Message, sferrors.ErrMsgUploadStreamSkipped)
}

func postUploadStreamPresignedQuery(t *testing.T, dstFileName, presigned string) string {
	t.Helper()
	var posted string
	sc := &snowflakeConn{
		cfg: &Config{},
		rest: &snowflakeRestful{
			FuncPostQuery: func(_ context.Context, _ *snowflakeRestful, _ *url.Values, _ map[string]string, body []byte, _ time.Duration, _ UUID, _ *Config) (*execResponse, error) {
				var req execRequest
				assertNilF(t, json.Unmarshal(body, &req))
				posted = req.SQLText
				return &execResponse{Data: execResponseData{
					StageInfo: execResponseStageInfo{
						LocationType: "GCS",
						PresignedURL: presigned,
					},
				}}, nil
			},
		},
	}
	sfa := &snowflakeFileTransferAgent{
		ctx:               context.Background(),
		sc:                sc,
		data:              &execResponseData{},
		command:           buildUploadStreamPutSQL("@mystage", "", true),
		commandType:       uploadCommand,
		stageLocationType: gcsClient,
		stageInfo:         &execResponseStageInfo{},
		streamUpload:      true,
		fileMetadata:      []*fileMetadata{{dstFileName: dstFileName}},
	}
	err := sfa.updateFileMetadataWithPresignedURL()
	if presignedURLTargetsDest(mustParseURL(t, presigned), dstFileName) {
		assertNilF(t, err)
		return posted
	}
	var se *SnowflakeError
	assertErrorsAsF(t, err, &se)
	assertEqualF(t, se.Number, ErrFailedToUploadToStage)
	assertStringContainsF(t, err.Error(), dstFileName, "mismatch names the destination")
	return posted
}

func mustParseURL(t *testing.T, raw string) *url.URL {
	t.Helper()
	u, err := url.Parse(raw)
	assertNilF(t, err)
	return u
}

func TestSpoolDoesNotCloseSource(t *testing.T) {
	src := &closeCounter{Reader: bytes.NewReader([]byte("hello"))}
	spool, err := spoolUploadStream(src, true, MaxBufferUnbounded, t.TempDir())
	assertNilF(t, err)
	assertEqualF(t, src.closed, 0)
	assertNotNilF(t, spool.buf)
	assertTrueF(t, len(spool.buf.Bytes()) > len("hello"), "gzip wrapper is larger than the plaintext")
}

func TestUploadStreamUncompressedRoundTrip(t *testing.T) {
	runSnowflakeConnTest(t, func(sct *SCTest) {
		contents := "upload,stream\n"
		stageDir := "test_upload_stream_" + randomString(10)
		src := &closeCounter{Reader: stringsReader(contents)}
		err := sct.sc.UploadStreamWithConfig(context.Background(), "@~", "data.txt", src, UploadStreamConfig{
			DestPrefix:  stageDir,
			Compression: CompressionNone,
		})
		assertNilF(t, err, "UploadStream")
		assertEqualF(t, src.closed, 0, "driver must not close the source")
		sct.Cleanup(func() {
			_, _ = sct.sc.Exec("rm @~/"+stageDir, nil)
		})

		r, err := sct.sc.DownloadStream(context.Background(), "@~/"+stageDir, "data.txt")
		assertNilF(t, err, "DownloadStream")
		defer func() {
			assertNilF(t, r.Close())
		}()
		got, err := io.ReadAll(r)
		assertNilF(t, err)
		assertEqualF(t, string(got), contents)
	})
}

func TestUploadStreamDefaultGzipRoundTrip(t *testing.T) {
	runSnowflakeConnTest(t, func(sct *SCTest) {
		contents := "gzip-me"
		stageDir := "test_upload_stream_gz_" + randomString(10)
		err := sct.sc.UploadStreamWithConfig(context.Background(), "@~", "hello.txt", stringsReader(contents), UploadStreamConfig{
			DestPrefix: stageDir,
		})
		assertNilF(t, err, "UploadStream")
		sct.Cleanup(func() {
			_, _ = sct.sc.Exec("rm @~/"+stageDir, nil)
		})

		plain, err := sct.sc.DownloadStreamWithConfig(context.Background(), "@~/"+stageDir, "hello.txt.gz", DownloadStreamConfig{Decompress: true})
		assertNilF(t, err, "DownloadStream decompress")
		defer func() {
			assertNilF(t, plain.Close())
		}()
		got, err := io.ReadAll(plain)
		assertNilF(t, err)
		assertEqualF(t, string(got), contents)

		stored, err := sct.sc.DownloadStream(context.Background(), "@~/"+stageDir, "hello.txt.gz")
		assertNilF(t, err, "DownloadStream stored bytes")
		defer func() {
			assertNilF(t, stored.Close())
		}()
		gzipBytes, err := io.ReadAll(stored)
		assertNilF(t, err)
		assertTrueF(t, len(gzipBytes) >= 2 && gzipBytes[0] == 0x1f && gzipBytes[1] == 0x8b, "expected gzip magic")
	})
}

func TestUploadStreamEncryptedMemoryAndDisk(t *testing.T) {
	runSnowflakeConnTest(t, func(sct *SCTest) {
		stageDir := "test_upload_stream_buf_" + randomString(10)
		sct.Cleanup(func() {
			_, _ = sct.sc.Exec("rm @~/"+stageDir, nil)
		})
		for _, tc := range []struct {
			name      string
			maxBuffer int64
			prefix    string
			dest      string
			contents  string
		}{
			{name: "disk", maxBuffer: 1, prefix: stageDir, dest: "disk.txt", contents: "from-disk"},
			{name: "memory", maxBuffer: MaxBufferUnbounded, prefix: stageDir + "/nested", dest: "memory.txt", contents: "from-memory"},
		} {
			err := sct.sc.UploadStreamWithConfig(context.Background(), "@~", tc.dest, stringsReader(tc.contents), UploadStreamConfig{
				DestPrefix:  tc.prefix,
				Compression: CompressionNone,
				MaxBuffer:   tc.maxBuffer,
			})
			assertNilF(t, err, tc.name)
			r, err := sct.sc.DownloadStream(context.Background(), "@~/"+tc.prefix, tc.dest)
			assertNilF(t, err, tc.name+" download")
			got, err := io.ReadAll(r)
			assertNilF(t, err, tc.name+" read")
			assertNilF(t, r.Close())
			assertEqualF(t, string(got), tc.contents, tc.name)
		}
	})
}

type readFlagReader struct {
	read bool
}

func (r *readFlagReader) Read(p []byte) (int, error) {
	r.read = true
	return 0, io.EOF
}

type bytesThenErr struct {
	b   []byte
	err error
	off int
}

func (r *bytesThenErr) Read(p []byte) (int, error) {
	if r.off >= len(r.b) {
		if r.err != nil {
			return 0, r.err
		}
		return 0, io.EOF
	}
	n := copy(p, r.b[r.off:])
	r.off += n
	return n, nil
}

type gcsBodyRecorder struct {
	bodies [][]byte
	status int
}

func (g *gcsBodyRecorder) Do(req *http.Request) (*http.Response, error) {
	body, err := io.ReadAll(req.Body)
	if err != nil {
		return nil, err
	}
	g.bodies = append(g.bodies, body)
	return &http.Response{
		StatusCode: g.status,
		Status:     "500 Internal Server Error",
		Body:       io.NopCloser(bytes.NewReader(nil)),
		Header:     make(http.Header),
	}, nil
}

type statusRoundTripper struct {
	status int
	calls  int
}

func (s *statusRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	s.calls++
	header := make(http.Header)
	header.Set("Content-Length", "4")
	return &http.Response{
		StatusCode: s.status,
		Status:     http.StatusText(s.status),
		Header:     header,
		Body:       io.NopCloser(bytes.NewReader(nil)),
		Request:    req,
	}, nil
}

type closeCounter struct {
	io.Reader
	closed int
}

func (c *closeCounter) Close() error {
	c.closed++
	return nil
}

func stringsReader(s string) io.Reader {
	return strings.NewReader(s)
}

func sha256Base64(b []byte) string {
	sum := sha256.Sum256(b)
	return base64.StdEncoding.EncodeToString(sum[:])
}

func assertUploadStreamInvalidArg(t *testing.T, err error) {
	t.Helper()
	assertNotNilF(t, err)
	var se *SnowflakeError
	assertErrorsAsF(t, err, &se)
	assertEqualF(t, se.Number, ErrUploadStreamInvalidArg)
}
