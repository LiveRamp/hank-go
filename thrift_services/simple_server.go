/*

Copies from the upstream thrift version before https://github.com/apache/thrift/commit/a576896398f03d1854f128479d31659446c51027#diff-883b034244fa515763cccc7e062a762c.

Test shutdown hangs forever if you try to end cleanly.  Feel free to fix upstream.  I don't have time.

*/

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package thrift_services

import (
	"git.apache.org/thrift.git/lib/go/thrift"
	"log"
	"runtime/debug"
	"sync"
)

/*
 * This is not a typical TSimpleServer as it is not blocked after accept a socket.
 * It is more like a TThreadedServer that can handle different connections in different goroutines.
 * This will work if golang user implements a conn-pool like thing in client side.
 */
type TSimpleServer struct {
	quit chan struct{}

	processorFactory       thrift.TProcessorFactory
	serverTransport        thrift.TServerTransport
	inputTransportFactory  thrift.TTransportFactory
	outputTransportFactory thrift.TTransportFactory
	inputProtocolFactory   thrift.TProtocolFactory
	outputProtocolFactory  thrift.TProtocolFactory
}

func NewTSimpleServer2(processor thrift.TProcessor, serverTransport thrift.TServerTransport) *TSimpleServer {
	return NewTSimpleServerFactory2(thrift.NewTProcessorFactory(processor), serverTransport)
}

func NewTSimpleServer4(processor thrift.TProcessor, serverTransport thrift.TServerTransport, transportFactory thrift.TTransportFactory, protocolFactory thrift.TProtocolFactory) *TSimpleServer {
	return NewTSimpleServerFactory4(thrift.NewTProcessorFactory(processor),
		serverTransport,
		transportFactory,
		protocolFactory,
	)
}

func NewTSimpleServer6(processor thrift.TProcessor, serverTransport thrift.TServerTransport, inputTransportFactory thrift.TTransportFactory, outputTransportFactory thrift.TTransportFactory, inputProtocolFactory thrift.TProtocolFactory, outputProtocolFactory thrift.TProtocolFactory) *TSimpleServer {
	return NewTSimpleServerFactory6(thrift.NewTProcessorFactory(processor),
		serverTransport,
		inputTransportFactory,
		outputTransportFactory,
		inputProtocolFactory,
		outputProtocolFactory,
	)
}

func NewTSimpleServerFactory2(processorFactory thrift.TProcessorFactory, serverTransport thrift.TServerTransport) *TSimpleServer {
	return NewTSimpleServerFactory6(processorFactory,
		serverTransport,
		thrift.NewTTransportFactory(),
		thrift.NewTTransportFactory(),
		thrift.NewTBinaryProtocolFactoryDefault(),
		thrift.NewTBinaryProtocolFactoryDefault(),
	)
}

func NewTSimpleServerFactory4(processorFactory thrift.TProcessorFactory, serverTransport thrift.TServerTransport, transportFactory thrift.TTransportFactory, protocolFactory thrift.TProtocolFactory) *TSimpleServer {
	return NewTSimpleServerFactory6(processorFactory,
		serverTransport,
		transportFactory,
		transportFactory,
		protocolFactory,
		protocolFactory,
	)
}

func NewTSimpleServerFactory6(processorFactory thrift.TProcessorFactory, serverTransport thrift.TServerTransport, inputTransportFactory thrift.TTransportFactory, outputTransportFactory thrift.TTransportFactory, inputProtocolFactory thrift.TProtocolFactory, outputProtocolFactory thrift.TProtocolFactory) *TSimpleServer {
	return &TSimpleServer{
		processorFactory:       processorFactory,
		serverTransport:        serverTransport,
		inputTransportFactory:  inputTransportFactory,
		outputTransportFactory: outputTransportFactory,
		inputProtocolFactory:   inputProtocolFactory,
		outputProtocolFactory:  outputProtocolFactory,
		quit: make(chan struct{}, 1),
	}
}

func (p *TSimpleServer) ProcessorFactory() thrift.TProcessorFactory {
	return p.processorFactory
}

func (p *TSimpleServer) ServerTransport() thrift.TServerTransport {
	return p.serverTransport
}

func (p *TSimpleServer) InputTransportFactory() thrift.TTransportFactory {
	return p.inputTransportFactory
}

func (p *TSimpleServer) OutputTransportFactory() thrift.TTransportFactory {
	return p.outputTransportFactory
}

func (p *TSimpleServer) InputProtocolFactory() thrift.TProtocolFactory {
	return p.inputProtocolFactory
}

func (p *TSimpleServer) OutputProtocolFactory() thrift.TProtocolFactory {
	return p.outputProtocolFactory
}

func (p *TSimpleServer) Listen() error {
	return p.serverTransport.Listen()
}

func (p *TSimpleServer) AcceptLoop() error {
	for {
		client, err := p.serverTransport.Accept()
		if err != nil {
			select {
			case <-p.quit:
				return nil
			default:
			}
			return err
		}
		if client != nil {
			go func() {
				if err := p.processRequests(client); err != nil {
					log.Println("error processing request:", err)
				}
			}()
		}
	}
}

func (p *TSimpleServer) Serve() error {
	err := p.Listen()
	if err != nil {
		return err
	}
	p.AcceptLoop()
	return nil
}

var once sync.Once

func (p *TSimpleServer) Stop() error {
	q := func() {
		p.quit <- struct{}{}
		p.serverTransport.Interrupt()
	}
	once.Do(q)
	return nil
}

func (p *TSimpleServer) processRequests(client thrift.TTransport) error {
	processor := p.processorFactory.GetProcessor(client)
	inputTransport := p.inputTransportFactory.GetTransport(client)
	outputTransport := p.outputTransportFactory.GetTransport(client)
	inputProtocol := p.inputProtocolFactory.GetProtocol(inputTransport)
	outputProtocol := p.outputProtocolFactory.GetProtocol(outputTransport)
	defer func() {
		if e := recover(); e != nil {
			log.Printf("panic in processor: %s: %s", e, debug.Stack())
		}
	}()
	if inputTransport != nil {
		defer inputTransport.Close()
	}
	if outputTransport != nil {
		defer outputTransport.Close()
	}
	for {
		ok, err := processor.Process(inputProtocol, outputProtocol)
		if err, ok := err.(thrift.TTransportException); ok && err.TypeId() == thrift.END_OF_FILE {
			return nil
		} else if err != nil {
			return err
		}
		if err, ok := err.(thrift.TApplicationException); ok && err.TypeId() == thrift.UNKNOWN_METHOD {
			continue
		}
		if !ok {
			break
		}
	}
	return nil
}
