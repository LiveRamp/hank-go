package iface

import "github.com/liveramp/hank/hank-core/src/main/go/hank"

func AsDomain(val interface{}) Domain {
	if val == nil {
		return nil
	}
	return val.(Domain)
}

func AsDomainGroupMetadata(val interface{}) *hank.DomainGroupMetadata {
	if val == nil {
		return nil
	}
	return val.(*hank.DomainGroupMetadata)
}

func AsHostMetadata(val interface{}) *hank.HostMetadata {
	if val == nil {
		return nil
	}
	return val.(*hank.HostMetadata)
}

func AsDomainMetadata(val interface{}) *hank.DomainMetadata {
	if val == nil {
		return nil
	}
	return val.(*hank.DomainMetadata)
}

//  watched thrift map cast copypasta

func AsDomainGroup(val interface{}) DomainGroup {
	if val == nil {
		return nil
	}
	return val.(DomainGroup)
}

func AsRingGroup(val interface{}) RingGroup {
	if val == nil {
		return nil
	}
	return val.(RingGroup)
}

func AsClientMetadata(val interface{}) *hank.ClientMetadata {
	if val == nil {
		return nil
	}
	return val.(*hank.ClientMetadata)
}

func AsRing(val interface{}) Ring {
	if val == nil {
		return nil
	}
	return val.(Ring)
}

func AsHostAssignmentsMetadata(val interface{}) *hank.HostAssignmentsMetadata {
	if val == nil {
		return nil
	}
	return val.(*hank.HostAssignmentsMetadata)
}

func AsHostDomainPartition(val interface{}) HostDomainPartition {
	if val == nil {
		return nil
	}
	return val.(HostDomainPartition)
}

//	stupid constructors required to return interface{}

func NewDomainGroupMetadata() interface{} {
	return hank.NewDomainGroupMetadata()
}

func NewDomainMetadata() interface{} {
	return hank.NewDomainMetadata()
}

func NewHostMetadata() interface{} {
	return hank.NewHostMetadata()
}

func NewHostAssignmentMetadata() interface{} {
	return hank.NewHostAssignmentsMetadata()
}
