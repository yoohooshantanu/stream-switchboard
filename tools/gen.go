//go:build ignore

package main

import (
	"fmt"
	"os"
	"path/filepath"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/descriptorpb"
)

func strPtr(s string) *string { return &s }
func int32Ptr(i int32) *int32 { return &i }

func main() {
	protoPackage := "audiogateway.v1"
	fileName := "audio_gateway.proto"

	fd := &descriptorpb.FileDescriptorProto{
		Name:    strPtr(fileName),
		Package: strPtr(protoPackage),
		Syntax:  strPtr("proto3"),
		Options: &descriptorpb.FileOptions{
			GoPackage: strPtr("github.com/yoohooshantanu/audio-gateway/internal/pb"),
		},
	}

	// 1. Enum EventType
	enumDesc := &descriptorpb.EnumDescriptorProto{
		Name: strPtr("EventType"),
		Value: []*descriptorpb.EnumValueDescriptorProto{
			{Name: strPtr("EVENT_UNKNOWN"), Number: int32Ptr(0)},
			{Name: strPtr("EVENT_TRANSCRIPT_PARTIAL"), Number: int32Ptr(1)},
			{Name: strPtr("EVENT_TRANSCRIPT_FINAL"), Number: int32Ptr(2)},
			{Name: strPtr("EVENT_SENTIMENT"), Number: int32Ptr(3)},
			{Name: strPtr("EVENT_INTENT_DETECTED"), Number: int32Ptr(4)},
			{Name: strPtr("EVENT_CALL_ENDED"), Number: int32Ptr(5)},
			{Name: strPtr("EVENT_CALL_ERROR"), Number: int32Ptr(6)},
		},
	}
	fd.EnumType = append(fd.EnumType, enumDesc)

	// 2. Message AudioChunk
	audioChunk := &descriptorpb.DescriptorProto{
		Name: strPtr("AudioChunk"),
		Field: []*descriptorpb.FieldDescriptorProto{
			{Name: strPtr("call_id"), Number: int32Ptr(1), Type: descriptorpb.FieldDescriptorProto_TYPE_STRING.Enum(), Label: descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum()},
			{Name: strPtr("tenant_id"), Number: int32Ptr(2), Type: descriptorpb.FieldDescriptorProto_TYPE_STRING.Enum(), Label: descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum()},
			{Name: strPtr("sequence"), Number: int32Ptr(3), Type: descriptorpb.FieldDescriptorProto_TYPE_UINT64.Enum(), Label: descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum()},
			{Name: strPtr("data"), Number: int32Ptr(4), Type: descriptorpb.FieldDescriptorProto_TYPE_BYTES.Enum(), Label: descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum()},
			{Name: strPtr("is_final"), Number: int32Ptr(5), Type: descriptorpb.FieldDescriptorProto_TYPE_BOOL.Enum(), Label: descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum()},
			{Name: strPtr("timestamp"), Number: int32Ptr(6), Type: descriptorpb.FieldDescriptorProto_TYPE_INT64.Enum(), Label: descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum()},
		},
	}
	fd.MessageType = append(fd.MessageType, audioChunk)

	// 3. Message CallEvent
	callEvent := &descriptorpb.DescriptorProto{
		Name: strPtr("CallEvent"),
		Field: []*descriptorpb.FieldDescriptorProto{
			{Name: strPtr("call_id"), Number: int32Ptr(1), Type: descriptorpb.FieldDescriptorProto_TYPE_STRING.Enum(), Label: descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum()},
			{Name: strPtr("type"), Number: int32Ptr(2), Type: descriptorpb.FieldDescriptorProto_TYPE_ENUM.Enum(), TypeName: strPtr(".audio-gateway.v1.EventType"), Label: descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum()},
			{Name: strPtr("payload"), Number: int32Ptr(3), Type: descriptorpb.FieldDescriptorProto_TYPE_STRING.Enum(), Label: descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum()},
			{Name: strPtr("sequence"), Number: int32Ptr(4), Type: descriptorpb.FieldDescriptorProto_TYPE_UINT64.Enum(), Label: descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum()},
			{Name: strPtr("timestamp"), Number: int32Ptr(5), Type: descriptorpb.FieldDescriptorProto_TYPE_INT64.Enum(), Label: descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum()},
		},
	}
	fd.MessageType = append(fd.MessageType, callEvent)

	// 4. Message WatchRequest
	watchReq := &descriptorpb.DescriptorProto{
		Name: strPtr("WatchRequest"),
		Field: []*descriptorpb.FieldDescriptorProto{
			{Name: strPtr("call_id"), Number: int32Ptr(1), Type: descriptorpb.FieldDescriptorProto_TYPE_STRING.Enum(), Label: descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum()},
			{Name: strPtr("tenant_id"), Number: int32Ptr(2), Type: descriptorpb.FieldDescriptorProto_TYPE_STRING.Enum(), Label: descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum()},
			{Name: strPtr("api_key"), Number: int32Ptr(3), Type: descriptorpb.FieldDescriptorProto_TYPE_STRING.Enum(), Label: descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum()},
		},
	}
	fd.MessageType = append(fd.MessageType, watchReq)

	// 5. Message SummaryRequest
	summaryReq := &descriptorpb.DescriptorProto{
		Name: strPtr("SummaryRequest"),
		Field: []*descriptorpb.FieldDescriptorProto{
			{Name: strPtr("call_id"), Number: int32Ptr(1), Type: descriptorpb.FieldDescriptorProto_TYPE_STRING.Enum(), Label: descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum()},
		},
	}
	fd.MessageType = append(fd.MessageType, summaryReq)

	// 6. Message CallSummary
	callSummary := &descriptorpb.DescriptorProto{
		Name: strPtr("CallSummary"),
		Field: []*descriptorpb.FieldDescriptorProto{
			{Name: strPtr("call_id"), Number: int32Ptr(1), Type: descriptorpb.FieldDescriptorProto_TYPE_STRING.Enum(), Label: descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum()},
			{Name: strPtr("transcript"), Number: int32Ptr(2), Type: descriptorpb.FieldDescriptorProto_TYPE_STRING.Enum(), Label: descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum()},
			{Name: strPtr("sentiment"), Number: int32Ptr(3), Type: descriptorpb.FieldDescriptorProto_TYPE_STRING.Enum(), Label: descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum()},
			{Name: strPtr("intent"), Number: int32Ptr(4), Type: descriptorpb.FieldDescriptorProto_TYPE_STRING.Enum(), Label: descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum()},
			{Name: strPtr("duration_ms"), Number: int32Ptr(5), Type: descriptorpb.FieldDescriptorProto_TYPE_INT64.Enum(), Label: descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum()},
			{Name: strPtr("total_chunks"), Number: int32Ptr(6), Type: descriptorpb.FieldDescriptorProto_TYPE_INT32.Enum(), Label: descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum()},
			{Name: strPtr("total_events"), Number: int32Ptr(7), Type: descriptorpb.FieldDescriptorProto_TYPE_INT32.Enum(), Label: descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum()},
		},
	}
	fd.MessageType = append(fd.MessageType, callSummary)

	// 7. Service AudioGateway
	isTrue := true
	svc := &descriptorpb.ServiceDescriptorProto{
		Name: strPtr("AudioGateway"),
		Method: []*descriptorpb.MethodDescriptorProto{
			{
				Name:            strPtr("StreamCall"),
				InputType:       strPtr(".audio-gateway.v1.AudioChunk"),
				OutputType:      strPtr(".audio-gateway.v1.CallEvent"),
				ClientStreaming: &isTrue,
				ServerStreaming: &isTrue,
			},
			{
				Name:            strPtr("WatchCall"),
				InputType:       strPtr(".audio-gateway.v1.WatchRequest"),
				OutputType:      strPtr(".audio-gateway.v1.CallEvent"),
				ServerStreaming: &isTrue,
			},
			{
				Name:       strPtr("GetCallSummary"),
				InputType:  strPtr(".audio-gateway.v1.SummaryRequest"),
				OutputType: strPtr(".audio-gateway.v1.CallSummary"),
			},
		},
	}
	fd.Service = append(fd.Service, svc)

	rawBytes, err := proto.Marshal(fd)
	if err != nil {
		fmt.Printf("Error marshaling fd: %v\n", err)
		os.Exit(1)
	}

	destDir := filepath.Join("internal", "pb")
	_ = os.MkdirAll(destDir, 0755)

	// Write internal/pb/audio-gateway.pb.go with uncompressed rawBytes
	pbGo := generatePBGo(rawBytes)
	if err := os.WriteFile(filepath.Join(destDir, "audio-gateway.pb.go"), []byte(pbGo), 0644); err != nil {
		fmt.Printf("Error writing pb.go: %v\n", err)
		os.Exit(1)
	}

	// Write internal/pb/audio_gateway_grpc.pb.go
	grpcGo := generateGRPCGo()
	if err := os.WriteFile(filepath.Join(destDir, "audio_gateway_grpc.pb.go"), []byte(grpcGo), 0644); err != nil {
		fmt.Printf("Error writing grpc.pb.go: %v\n", err)
		os.Exit(1)
	}

	fmt.Println("Generated internal/pb/audio-gateway.pb.go and audio_gateway_grpc.pb.go successfully!")
}

func generatePBGo(gzippedDesc []byte) string {
	hexBytes := ""
	for i, b := range gzippedDesc {
		if i%12 == 0 {
			hexBytes += "\n\t"
		}
		hexBytes += fmt.Sprintf("0x%02x, ", b)
	}

	return fmt.Sprintf(`// Code generated by protoc-gen-go. DO NOT EDIT.
// versions:
// 	protoc-gen-go v1.34.2
// 	protoc        v5.27.1
// source: audio_gateway.proto

package pb

import (
	protoreflect "google.golang.org/protobuf/reflect/protoreflect"
	protoimpl "google.golang.org/protobuf/runtime/protoimpl"
	reflect "reflect"
	sync "sync"
)

const (
	_ = protoimpl.EnforceVersion(20 - protoimpl.MinVersion)
	_ = protoimpl.EnforceVersion(protoimpl.MaxVersion - 20)
)

type EventType int32

const (
	EventType_EVENT_UNKNOWN            EventType = 0
	EventType_EVENT_TRANSCRIPT_PARTIAL EventType = 1
	EventType_EVENT_TRANSCRIPT_FINAL   EventType = 2
	EventType_EVENT_SENTIMENT          EventType = 3
	EventType_EVENT_INTENT_DETECTED    EventType = 4
	EventType_EVENT_CALL_ENDED         EventType = 5
	EventType_EVENT_CALL_ERROR         EventType = 6
)

var (
	EventType_name = map[int32]string{
		0: "EVENT_UNKNOWN",
		1: "EVENT_TRANSCRIPT_PARTIAL",
		2: "EVENT_TRANSCRIPT_FINAL",
		3: "EVENT_SENTIMENT",
		4: "EVENT_INTENT_DETECTED",
		5: "EVENT_CALL_ENDED",
		6: "EVENT_CALL_ERROR",
	}
	EventType_value = map[string]int32{
		"EVENT_UNKNOWN":            0,
		"EVENT_TRANSCRIPT_PARTIAL": 1,
		"EVENT_TRANSCRIPT_FINAL":   2,
		"EVENT_SENTIMENT":          3,
		"EVENT_INTENT_DETECTED":    4,
		"EVENT_CALL_ENDED":         5,
		"EVENT_CALL_ERROR":         6,
	}
)

func (x EventType) Enum() *EventType {
	p := new(EventType)
	*p = x
	return p
}

func (x EventType) String() string {
	return protoimpl.X.EnumStringOf(x.Descriptor(), protoreflect.EnumNumber(x))
}

func (EventType) Descriptor() protoreflect.EnumDescriptor {
	return file_audio_gateway_proto_enumTypes[0].Descriptor()
}

func (EventType) Type() protoreflect.EnumType {
	return &file_audio_gateway_proto_enumTypes[0]
}

func (x EventType) Number() protoreflect.EnumNumber {
	return protoreflect.EnumNumber(x)
}

func (EventType) EnumDescriptor() ([]byte, []int) {
	return file_audio_gateway_proto_rawDescGZIP(), []int{0}
}

// AudioChunk represents a chunk of streamed audio payload
type AudioChunk struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	CallId    string ` + "`" + `protobuf:"bytes,1,opt,name=call_id,json=callId,proto3" json:"call_id,omitempty"` + "`" + `
	TenantId  string ` + "`" + `protobuf:"bytes,2,opt,name=tenant_id,json=tenantId,proto3" json:"tenant_id,omitempty"` + "`" + `
	Sequence  uint64 ` + "`" + `protobuf:"varint,3,opt,name=sequence,proto3" json:"sequence,omitempty"` + "`" + `
	Data      []byte ` + "`" + `protobuf:"bytes,4,opt,name=data,proto3" json:"data,omitempty"` + "`" + `
	IsFinal   bool   ` + "`" + `protobuf:"varint,5,opt,name=is_final,json=isFinal,proto3" json:"is_final,omitempty"` + "`" + `
	Timestamp int64  ` + "`" + `protobuf:"varint,6,opt,name=timestamp,proto3" json:"timestamp,omitempty"` + "`" + `
}

func (x *AudioChunk) Reset() {
	*x = AudioChunk{}
	if protoimpl.UnsafeEnabled {
		mi := &file_audio_gateway_proto_msgTypes[0]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *AudioChunk) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*AudioChunk) ProtoMessage() {}

func (x *AudioChunk) ProtoReflect() protoreflect.Message {
	mi := &file_audio_gateway_proto_msgTypes[0]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

func (*AudioChunk) Descriptor() ([]byte, []int) {
	return file_audio_gateway_proto_rawDescGZIP(), []int{0}
}

func (x *AudioChunk) GetCallId() string {
	if x != nil {
		return x.CallId
	}
	return ""
}

func (x *AudioChunk) GetTenantId() string {
	if x != nil {
		return x.TenantId
	}
	return ""
}

func (x *AudioChunk) GetSequence() uint64 {
	if x != nil {
		return x.Sequence
	}
	return 0
}

func (x *AudioChunk) GetData() []byte {
	if x != nil {
		return x.Data
	}
	return nil
}

func (x *AudioChunk) GetIsFinal() bool {
	if x != nil {
		return x.IsFinal
	}
	return false
}

func (x *AudioChunk) GetTimestamp() int64 {
	if x != nil {
		return x.Timestamp
	}
	return 0
}

// CallEvent represents an event emitted downstream
type CallEvent struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	CallId    string    ` + "`" + `protobuf:"bytes,1,opt,name=call_id,json=callId,proto3" json:"call_id,omitempty"` + "`" + `
	Type      EventType ` + "`" + `protobuf:"varint,2,opt,name=type,proto3,enum=audio-gateway.v1.EventType" json:"type,omitempty"` + "`" + `
	Payload   string    ` + "`" + `protobuf:"bytes,3,opt,name=payload,proto3" json:"payload,omitempty"` + "`" + `
	Sequence  uint64    ` + "`" + `protobuf:"varint,4,opt,name=sequence,proto3" json:"sequence,omitempty"` + "`" + `
	Timestamp int64     ` + "`" + `protobuf:"varint,5,opt,name=timestamp,proto3" json:"timestamp,omitempty"` + "`" + `
}

func (x *CallEvent) Reset() {
	*x = CallEvent{}
	if protoimpl.UnsafeEnabled {
		mi := &file_audio_gateway_proto_msgTypes[1]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *CallEvent) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*CallEvent) ProtoMessage() {}

func (x *CallEvent) ProtoReflect() protoreflect.Message {
	mi := &file_audio_gateway_proto_msgTypes[1]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

func (*CallEvent) Descriptor() ([]byte, []int) {
	return file_audio_gateway_proto_rawDescGZIP(), []int{1}
}

func (x *CallEvent) GetCallId() string {
	if x != nil {
		return x.CallId
	}
	return ""
}

func (x *CallEvent) GetType() EventType {
	if x != nil {
		return x.Type
	}
	return EventType_EVENT_UNKNOWN
}

func (x *CallEvent) GetPayload() string {
	if x != nil {
		return x.Payload
	}
	return ""
}

func (x *CallEvent) GetSequence() uint64 {
	if x != nil {
		return x.Sequence
	}
	return 0
}

func (x *CallEvent) GetTimestamp() int64 {
	if x != nil {
		return x.Timestamp
	}
	return 0
}

// WatchRequest initiates a server-side stream subscription
type WatchRequest struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	CallId   string ` + "`" + `protobuf:"bytes,1,opt,name=call_id,json=callId,proto3" json:"call_id,omitempty"` + "`" + `
	TenantId string ` + "`" + `protobuf:"bytes,2,opt,name=tenant_id,json=tenantId,proto3" json:"tenant_id,omitempty"` + "`" + `
	ApiKey   string ` + "`" + `protobuf:"bytes,3,opt,name=api_key,json=apiKey,proto3" json:"api_key,omitempty"` + "`" + `
}

func (x *WatchRequest) Reset() {
	*x = WatchRequest{}
	if protoimpl.UnsafeEnabled {
		mi := &file_audio_gateway_proto_msgTypes[2]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *WatchRequest) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*WatchRequest) ProtoMessage() {}

func (x *WatchRequest) ProtoReflect() protoreflect.Message {
	mi := &file_audio_gateway_proto_msgTypes[2]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

func (*WatchRequest) Descriptor() ([]byte, []int) {
	return file_audio_gateway_proto_rawDescGZIP(), []int{2}
}

func (x *WatchRequest) GetCallId() string {
	if x != nil {
		return x.CallId
	}
	return ""
}

func (x *WatchRequest) GetTenantId() string {
	if x != nil {
		return x.TenantId
	}
	return ""
}

func (x *WatchRequest) GetApiKey() string {
	if x != nil {
		return x.ApiKey
	}
	return ""
}

// SummaryRequest queries post-call metrics
type SummaryRequest struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	CallId string ` + "`" + `protobuf:"bytes,1,opt,name=call_id,json=callId,proto3" json:"call_id,omitempty"` + "`" + `
}

func (x *SummaryRequest) Reset() {
	*x = SummaryRequest{}
	if protoimpl.UnsafeEnabled {
		mi := &file_audio_gateway_proto_msgTypes[3]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *SummaryRequest) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*SummaryRequest) ProtoMessage() {}

func (x *SummaryRequest) ProtoReflect() protoreflect.Message {
	mi := &file_audio_gateway_proto_msgTypes[3]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

func (*SummaryRequest) Descriptor() ([]byte, []int) {
	return file_audio_gateway_proto_rawDescGZIP(), []int{3}
}

func (x *SummaryRequest) GetCallId() string {
	if x != nil {
		return x.CallId
	}
	return ""
}

// CallSummary contains aggregated analytics for a completed call
type CallSummary struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	CallId      string ` + "`" + `protobuf:"bytes,1,opt,name=call_id,json=callId,proto3" json:"call_id,omitempty"` + "`" + `
	Transcript  string ` + "`" + `protobuf:"bytes,2,opt,name=transcript,proto3" json:"transcript,omitempty"` + "`" + `
	Sentiment   string ` + "`" + `protobuf:"bytes,3,opt,name=sentiment,proto3" json:"sentiment,omitempty"` + "`" + `
	Intent      string ` + "`" + `protobuf:"bytes,4,opt,name=intent,proto3" json:"intent,omitempty"` + "`" + `
	DurationMs  int64  ` + "`" + `protobuf:"varint,5,opt,name=duration_ms,json=durationMs,proto3" json:"duration_ms,omitempty"` + "`" + `
	TotalChunks int32  ` + "`" + `protobuf:"varint,6,opt,name=total_chunks,json=totalChunks,proto3" json:"total_chunks,omitempty"` + "`" + `
	TotalEvents int32  ` + "`" + `protobuf:"varint,7,opt,name=total_events,json=totalEvents,proto3" json:"total_events,omitempty"` + "`" + `
}

func (x *CallSummary) Reset() {
	*x = CallSummary{}
	if protoimpl.UnsafeEnabled {
		mi := &file_audio_gateway_proto_msgTypes[4]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *CallSummary) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*CallSummary) ProtoMessage() {}

func (x *CallSummary) ProtoReflect() protoreflect.Message {
	mi := &file_audio_gateway_proto_msgTypes[4]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

func (*CallSummary) Descriptor() ([]byte, []int) {
	return file_audio_gateway_proto_rawDescGZIP(), []int{4}
}

func (x *CallSummary) GetCallId() string {
	if x != nil {
		return x.CallId
	}
	return ""
}

func (x *CallSummary) GetTranscript() string {
	if x != nil {
		return x.Transcript
	}
	return ""
}

func (x *CallSummary) GetSentiment() string {
	if x != nil {
		return x.Sentiment
	}
	return ""
}

func (x *CallSummary) GetIntent() string {
	if x != nil {
		return x.Intent
	}
	return ""
}

func (x *CallSummary) GetDurationMs() int64 {
	if x != nil {
		return x.DurationMs
	}
	return 0
}

func (x *CallSummary) GetTotalChunks() int32 {
	if x != nil {
		return x.TotalChunks
	}
	return 0
}

func (x *CallSummary) GetTotalEvents() int32 {
	if x != nil {
		return x.TotalEvents
	}
	return 0
}

var File_audio_gateway_proto protoreflect.FileDescriptor

var file_audio_gateway_proto_rawDesc = []byte{%s
}

var (
	file_audio_gateway_proto_rawDescOnce sync.Once
	file_audio_gateway_proto_rawDescData = file_audio_gateway_proto_rawDesc
)

func file_audio_gateway_proto_rawDescGZIP() []byte {
	file_audio_gateway_proto_rawDescOnce.Do(func() {
		file_audio_gateway_proto_rawDescData = protoimpl.X.CompressGZIP(file_audio_gateway_proto_rawDescData)
	})
	return file_audio_gateway_proto_rawDescData
}

var file_audio_gateway_proto_enumTypes = make([]protoimpl.EnumInfo, 1)
var file_audio_gateway_proto_msgTypes = make([]protoimpl.MessageInfo, 5)
var file_audio_gateway_proto_goTypes = []any{
	(EventType)(0),         // 0: audio-gateway.v1.EventType
	(*AudioChunk)(nil),     // 1: audio-gateway.v1.AudioChunk
	(*CallEvent)(nil),      // 2: audio-gateway.v1.CallEvent
	(*WatchRequest)(nil),   // 3: audio-gateway.v1.WatchRequest
	(*SummaryRequest)(nil), // 4: audio-gateway.v1.SummaryRequest
	(*CallSummary)(nil),    // 5: audio-gateway.v1.CallSummary
}
var file_audio_gateway_proto_depIdxs = []int32{
	0, // 0: audio-gateway.v1.CallEvent.type:type_name -> audio-gateway.v1.EventType
	1, // 1: audio-gateway.v1.AudioGateway.StreamCall:input_type -> audio-gateway.v1.AudioChunk
	3, // 2: audio-gateway.v1.AudioGateway.WatchCall:input_type -> audio-gateway.v1.WatchRequest
	4, // 3: audio-gateway.v1.AudioGateway.GetCallSummary:input_type -> audio-gateway.v1.SummaryRequest
	2, // 4: audio-gateway.v1.AudioGateway.StreamCall:output_type -> audio-gateway.v1.CallEvent
	2, // 5: audio-gateway.v1.AudioGateway.WatchCall:output_type -> audio-gateway.v1.CallEvent
	5, // 6: audio-gateway.v1.AudioGateway.GetCallSummary:output_type -> audio-gateway.v1.CallSummary
	4, // [4:7] is the sub-list for method output_type
	1, // [1:4] is the sub-list for method input_type
	1, // [1:1] is the sub-list for extension type_name
	1, // [1:1] is the sub-list for extension extendee
	0, // [0:1] is the sub-list for field type_name
}

func init() { file_audio_gateway_proto_init() }
func file_audio_gateway_proto_init() {
	if File_audio_gateway_proto != nil {
		return
	}
	type x struct{}
	out := protoimpl.TypeBuilder{
		File: protoimpl.DescBuilder{
			GoPackagePath: reflect.TypeOf(x{}).PkgPath(),
			RawDescriptor: file_audio_gateway_proto_rawDesc,
			NumEnums:      1,
			NumMessages:   5,
			NumExtensions: 0,
			NumServices:   1,
		},
		GoTypes:           file_audio_gateway_proto_goTypes,
		DependencyIndexes: file_audio_gateway_proto_depIdxs,
		EnumInfos:         file_audio_gateway_proto_enumTypes,
		MessageInfos:      file_audio_gateway_proto_msgTypes,
	}.Build()
	File_audio_gateway_proto = out.File
	file_audio_gateway_proto_rawDesc = nil
	file_audio_gateway_proto_goTypes = nil
	file_audio_gateway_proto_depIdxs = nil
}
`, hexBytes)
}

func generateGRPCGo() string {
	return `// Code generated by protoc-gen-go-grpc. DO NOT EDIT.
// versions:
// - protoc-gen-go-grpc v1.5.1
// - protoc             v5.27.1
// source: audio_gateway.proto

package pb

import (
	context "context"
	grpc "google.golang.org/grpc"
	codes "google.golang.org/grpc/codes"
	status "google.golang.org/grpc/status"
)

// This is a compile-time assertion to ensure that this generated file
// is compatible with the grpc package it is being compiled against.
// Requires gRPC-Go v1.64.0 or later.
const _ = grpc.SupportPackageIsVersion9

const (
	AudioGateway_StreamCall_FullMethodName     = "/audio-gateway.v1.AudioGateway/StreamCall"
	AudioGateway_WatchCall_FullMethodName      = "/audio-gateway.v1.AudioGateway/WatchCall"
	AudioGateway_GetCallSummary_FullMethodName = "/audio-gateway.v1.AudioGateway/GetCallSummary"
)

// AudioGatewayClient is the client API for AudioGateway service.
type AudioGatewayClient interface {
	StreamCall(ctx context.Context, opts ...grpc.CallOption) (grpc.BidiStreamingClient[AudioChunk, CallEvent], error)
	WatchCall(ctx context.Context, in *WatchRequest, opts ...grpc.CallOption) (grpc.ServerStreamingClient[CallEvent], error)
	GetCallSummary(ctx context.Context, in *SummaryRequest, opts ...grpc.CallOption) (*CallSummary, error)
}

type streamBridgeClient struct {
	cc grpc.ClientConnInterface
}

func NewAudioGatewayClient(cc grpc.ClientConnInterface) AudioGatewayClient {
	return &streamBridgeClient{cc}
}

func (c *streamBridgeClient) StreamCall(ctx context.Context, opts ...grpc.CallOption) (grpc.BidiStreamingClient[AudioChunk, CallEvent], error) {
	cOpts := append([]grpc.CallOption{grpc.StaticMethod()}, opts...)
	stream, err := c.cc.NewStream(ctx, &AudioGateway_ServiceDesc.Streams[0], AudioGateway_StreamCall_FullMethodName, cOpts...)
	if err != nil {
		return nil, err
	}
	x := &streamBridgeStreamCallClient{ClientStream: stream}
	return x, nil
}

type AudioGateway_StreamCallClient = grpc.BidiStreamingClient[AudioChunk, CallEvent]

type streamBridgeStreamCallClient struct {
	grpc.ClientStream
}

func (x *streamBridgeStreamCallClient) Send(m *AudioChunk) error {
	return x.ClientStream.SendMsg(m)
}

func (x *streamBridgeStreamCallClient) Recv() (*CallEvent, error) {
	m := new(CallEvent)
	if err := x.ClientStream.RecvMsg(m); err != nil {
		return nil, err
	}
	return m, nil
}

func (c *streamBridgeClient) WatchCall(ctx context.Context, in *WatchRequest, opts ...grpc.CallOption) (grpc.ServerStreamingClient[CallEvent], error) {
	cOpts := append([]grpc.CallOption{grpc.StaticMethod()}, opts...)
	stream, err := c.cc.NewStream(ctx, &AudioGateway_ServiceDesc.Streams[1], AudioGateway_WatchCall_FullMethodName, cOpts...)
	if err != nil {
		return nil, err
	}
	x := &streamBridgeWatchCallClient{ClientStream: stream}
	if err := x.ClientStream.SendMsg(in); err != nil {
		return nil, err
	}
	if err := x.ClientStream.CloseSend(); err != nil {
		return nil, err
	}
	return x, nil
}

type AudioGateway_WatchCallClient = grpc.ServerStreamingClient[CallEvent]

type streamBridgeWatchCallClient struct {
	grpc.ClientStream
}

func (x *streamBridgeWatchCallClient) Recv() (*CallEvent, error) {
	m := new(CallEvent)
	if err := x.ClientStream.RecvMsg(m); err != nil {
		return nil, err
	}
	return m, nil
}

func (c *streamBridgeClient) GetCallSummary(ctx context.Context, in *SummaryRequest, opts ...grpc.CallOption) (*CallSummary, error) {
	cOpts := append([]grpc.CallOption{grpc.StaticMethod()}, opts...)
	out := new(CallSummary)
	err := c.cc.Invoke(ctx, AudioGateway_GetCallSummary_FullMethodName, in, out, cOpts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

// AudioGatewayServer is the server API for AudioGateway service.
type AudioGatewayServer interface {
	StreamCall(grpc.BidiStreamingServer[AudioChunk, CallEvent]) error
	WatchCall(*WatchRequest, grpc.ServerStreamingServer[CallEvent]) error
	GetCallSummary(context.Context, *SummaryRequest) (*CallSummary, error)
	mustEmbedUnimplementedAudioGatewayServer()
}

// UnimplementedAudioGatewayServer must be embedded to have forward compatible implementations.
type UnimplementedAudioGatewayServer struct{}

func (UnimplementedAudioGatewayServer) StreamCall(grpc.BidiStreamingServer[AudioChunk, CallEvent]) error {
	return status.Errorf(codes.Unimplemented, "method StreamCall not implemented")
}
func (UnimplementedAudioGatewayServer) WatchCall(*WatchRequest, grpc.ServerStreamingServer[CallEvent]) error {
	return status.Errorf(codes.Unimplemented, "method WatchCall not implemented")
}
func (UnimplementedAudioGatewayServer) GetCallSummary(context.Context, *SummaryRequest) (*CallSummary, error) {
	return nil, status.Errorf(codes.Unimplemented, "method GetCallSummary not implemented")
}
func (UnimplementedAudioGatewayServer) mustEmbedUnimplementedAudioGatewayServer() {}

func RegisterAudioGatewayServer(s grpc.ServiceRegistrar, srv AudioGatewayServer) {
	s.RegisterService(&AudioGateway_ServiceDesc, srv)
}

func _AudioGateway_StreamCall_Handler(srv any, stream grpc.ServerStream) error {
	return srv.(AudioGatewayServer).StreamCall(&grpc.GenericServerStream[AudioChunk, CallEvent]{ServerStream: stream})
}

func _AudioGateway_WatchCall_Handler(srv any, stream grpc.ServerStream) error {
	m := new(WatchRequest)
	if err := stream.RecvMsg(m); err != nil {
		return err
	}
	return srv.(AudioGatewayServer).WatchCall(m, &grpc.GenericServerStream[WatchRequest, CallEvent]{ServerStream: stream})
}

func _AudioGateway_GetCallSummary_Handler(srv any, ctx context.Context, dec func(any) error, interceptor grpc.UnaryServerInterceptor) (any, error) {
	in := new(SummaryRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(AudioGatewayServer).GetCallSummary(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: AudioGateway_GetCallSummary_FullMethodName,
	}
	handler := func(ctx context.Context, req any) (any, error) {
		return srv.(AudioGatewayServer).GetCallSummary(ctx, req.(*SummaryRequest))
	}
	return interceptor(ctx, in, info, handler)
}

var AudioGateway_ServiceDesc = grpc.ServiceDesc{
	ServiceName: "audiogateway.v1.AudioGateway",
	HandlerType: (*AudioGatewayServer)(nil),
	Methods: []grpc.MethodDesc{
		{
			MethodName: "GetCallSummary",
			Handler:    _AudioGateway_GetCallSummary_Handler,
		},
	},
	Streams: []grpc.StreamDesc{
		{
			StreamName:    "StreamCall",
			Handler:       _AudioGateway_StreamCall_Handler,
			ServerStreams: true,
			ClientStreams: true,
		},
		{
			StreamName:    "WatchCall",
			Handler:       _AudioGateway_WatchCall_Handler,
			ServerStreams: true,
		},
	},
	Metadata: "audio_gateway.proto",
}
`
}
