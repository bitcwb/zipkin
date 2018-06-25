const {SPAN_V1} = require('../js/spanConverter');
const should = require('chai').should();

describe('SPAN v1 Conversion', () => {
  // endpoints from zipkin2.TestObjects
  const frontend = {
    serviceName: 'frontend',
    ipv4: '127.0.0.1',
    port: 8080
  };

  const backend = {
    serviceName: 'backend',
    ipv4: '192.168.99.101',
    port: 9000
  };

  // originally zipkin2.v1.SpanConverterTest.client
  it('converts client span', () => {
    const v2 = {
      traceId: '1',
      parentId: '2',
      id: '3',
      name: 'get',
      kind: 'CLIENT',
      timestamp: 1472470996199000,
      duration: 207000,
      localEndpoint: frontend,
      remoteEndpoint: backend,
      annotations: [
        {
          value: 'ws',
          timestamp: 1472470996238000
        },
        {
          value: 'wr',
          timestamp: 1472470996403000
        }
      ],
      tags: {
        'http.path': '/api',
        'clnt/finagle.version': '6.45.0',
      }
    };

    const expected = {
      traceId: '1',
      parentId: '2',
      id: '3',
      name: 'get',
      timestamp: 1472470996199000,
      duration: 207000,
      annotations: [
        {
          value: 'cs',
          timestamp: 1472470996199000,
          endpoint: frontend
        },
        {
          value: 'ws',
          timestamp: 1472470996238000,
          endpoint: frontend
        },
        {
          value: 'wr',
          timestamp: 1472470996403000,
          endpoint: frontend
        },
        {
          value: 'cr',
          timestamp: 1472470996406000,
          endpoint: frontend
        }
      ],
      binaryAnnotations: [
        {
          key: 'http.path',
          value: '/api',
          endpoint: frontend
        },
        {
          key: 'clnt/finagle.version',
          value: '6.45.0',
          endpoint: frontend
        },
        {
          key: 'sa',
          value: true,
          endpoint: backend
        }
      ]
    };

    const v1 = SPAN_V1.convert(v2);
    expect(v1).to.deep.equal(expected);
  });

  // originally zipkin2.v1.SpanConverterTest.SpanConverterTest.client_unfinished
  it('converts incomplete client span', () => {
    const v2 = {
      traceId: '1',
      parentId: '2',
      id: '3',
      name: 'get',
      kind: 'CLIENT',
      timestamp: 1472470996199000,
      localEndpoint: frontend,
      annotations: [
        {
          value: 'ws',
          timestamp: 1472470996238000
        }
      ]
    };

    const expected = {
      traceId: '1',
      parentId: '2',
      id: '3',
      name: 'get',
      timestamp: 1472470996199000,
      annotations: [
        {
          value: 'cs',
          timestamp: 1472470996199000,
          endpoint: frontend
        },
        {
          value: 'ws',
          timestamp: 1472470996238000,
          endpoint: frontend
        }
      ],
      binaryAnnotations: [] // prefers empty array to nil
    };

    const v1 = SPAN_V1.convert(v2);
    expect(v1).to.deep.equal(expected);
  });

  // originally zipkin2.v1.SpanConverterTest.client_kindInferredFromAnnotation
  it('infers cr annotation', () => {
    const v2 = {
      traceId: '1',
      parentId: '2',
      id: '3',
      name: 'get',
      timestamp: 1472470996199000,
      duration: 207000,
      localEndpoint: frontend,
      annotations: [
        {
          value: 'cs',
          timestamp: 1472470996199000
        }
      ]
    };

    const expected = {
      traceId: '1',
      parentId: '2',
      id: '3',
      name: 'get',
      timestamp: 1472470996199000,
      duration: 207000,
      annotations: [
        {
          value: 'cs',
          timestamp: 1472470996199000,
          endpoint: frontend
        },
        {
          value: 'cr',
          timestamp: 1472470996406000,
          endpoint: frontend
        }
      ],
      binaryAnnotations: []
    };

    const v1 = SPAN_V1.convert(v2);
    expect(v1).to.deep.equal(expected);
  });

  // originally zipkin2.v1.SpanConverterTest.noAnnotationsExceptAddresses
  it('converts when remoteEndpoint exist without kind', () => {
    const v2 = {
      traceId: '1',
      parentId: '2',
      id: '3',
      name: 'get',
      timestamp: 1472470996199000,
      duration: 207000,
      localEndpoint: frontend,
      remoteEndpoint: backend
    };

    const expected = {
      traceId: '1',
      parentId: '2',
      id: '3',
      name: 'get',
      timestamp: 1472470996199000,
      duration: 207000,
      annotations: [],
      binaryAnnotations: [
        {
          key: 'lc',
          value: '',
          endpoint: frontend
        },
        {
          key: 'sa',
          value: true,
          endpoint: backend
        }
      ]
    };

    const v1 = SPAN_V1.convert(v2);
    expect(v1).to.deep.equal(expected);
  });

  // originally zipkin2.v1.SpanConverterTest.server
  it('converts root server span', () => {
    // let's pretend there was no caller, so we don't set shared flag
    const v2 = {
      traceId: '1',
      id: '2',
      name: 'get',
      kind: 'SERVER',
      localEndpoint: backend,
      remoteEndpoint: frontend,
      timestamp: 1472470996199000,
      duration: 207000,
      tags: {
        'http.path': '/api',
        'finagle.version': '6.45.0'
      }
    };

    const expected = {
      traceId: '1',
      id: '2',
      name: 'get',
      timestamp: 1472470996199000,
      duration: 207000,
      annotations: [
        {
          value: 'sr',
          timestamp: 1472470996199000,
          endpoint: backend
        },
        {
          value: 'ss',
          timestamp: 1472470996406000,
          endpoint: backend
        }
      ],
      binaryAnnotations: [
        {
          key: 'http.path',
          value: '/api',
          endpoint: backend
        },
        {
          key: 'finagle.version',
          value: '6.45.0',
          endpoint: backend
        },
        {
          key: 'ca',
          value: true,
          endpoint: frontend
        }
      ]
    };

    const v1 = SPAN_V1.convert(v2);
    expect(v1).to.deep.equal(expected);
  });

  // originally zipkin2.v1.SpanConverterTest.missingEndpoints
  it('converts span with no endpoints', () => {
    const v2 = {
      traceId: '1',
      parentId: '1',
      id: '2',
      name: 'foo',
      timestamp: 1472470996199000,
      duration: 207000
    };

    const expected = {
      traceId: '1',
      parentId: '1',
      id: '2',
      name: 'foo',
      timestamp: 1472470996199000,
      duration: 207000,
      annotations: [],
      binaryAnnotations: []
    };

    const v1 = SPAN_V1.convert(v2);
    expect(v1).to.deep.equal(expected);
  });

  // originally zipkin2.v1.SpanConverterTest.coreAnnotation
  it('converts v2 span retaining an sr annotation', () => {
    const v2 = {
      traceId: '1',
      parentId: '1',
      id: '2',
      name: 'foo',
      timestamp: 1472470996199000,
      annotations: [
        {
          value: 'cs',
          timestamp: 1472470996199000
        }
      ]
    };

    const expected = {
      traceId: '1',
      parentId: '1',
      id: '2',
      name: 'foo',
      timestamp: 1472470996199000,
      annotations: [
        {
          value: 'cs',
          timestamp: 1472470996199000
        }
      ],
      binaryAnnotations: []
    };

    const v1 = SPAN_V1.convert(v2);
    expect(v1).to.deep.equal(expected);
  });

  it('converts incomplete shared server span', () => {
    const v2 = {
      traceId: '1',
      parentId: '2',
      id: '3',
      name: 'get',
      kind: 'SERVER',
      shared: true,
      localEndpoint: backend,
      timestamp: 1472470996199000
    };

    const expected = {
      traceId: '1',
      parentId: '2',
      id: '3',
      name: 'get',
      annotations: [
        {
          value: 'sr',
          timestamp: 1472470996199000,
          endpoint: backend
        }
      ],
      binaryAnnotations: []
    };

    const v1 = SPAN_V1.convert(v2);
    expect(v1).to.deep.equal(expected);
  });

  it('converts producer span', () => {
    const v2 = {
      traceId: 'a',
      name: 'publish',
      id: 'c',
      parentId: 'b',
      kind: 'PRODUCER',
      timestamp: 1,
      duration: 1,
      localEndpoint: {serviceName: 'frontend'},
      remoteEndpoint: {serviceName: 'rabbitmq'}
    };

    const expected = {
      traceId: 'a',
      parentId: 'b',
      id: 'c',
      name: 'publish',
      timestamp: 1,
      duration: 1,
      annotations: [
        {value: 'ms', timestamp: 1, endpoint: {serviceName: 'frontend'}},
        {value: 'ws', timestamp: 2, endpoint: {serviceName: 'frontend'}},
      ],
      binaryAnnotations: [
        {
          key: 'ma',
          value: true,
          endpoint: {serviceName: 'rabbitmq'}
        }
      ]
    };

    const v1 = SPAN_V1.convert(v2);
    expect(v1).to.deep.equal(expected);
  });

  it('converts incomplete producer span', () => {
    const v2 = {
      traceId: 'a',
      name: 'publish',
      id: 'c',
      parentId: 'b',
      kind: 'PRODUCER',
      timestamp: 1,
      localEndpoint: {serviceName: 'frontend'}
    };

    const expected = {
      traceId: 'a',
      parentId: 'b',
      id: 'c',
      name: 'publish',
      timestamp: 1,
      annotations: [{value: 'ms', timestamp: 1, endpoint: {serviceName: 'frontend'}}],
      binaryAnnotations: []
    };

    const v1 = SPAN_V1.convert(v2);
    expect(v1).to.deep.equal(expected);
  });

  it('converts consumer span', () => {
    const v2 = {
      traceId: 'a',
      name: 'next-message',
      id: 'c',
      parentId: 'b',
      kind: 'CONSUMER',
      timestamp: 1,
      duration: 1,
      localEndpoint: {serviceName: 'backend'},
      remoteEndpoint: {serviceName: 'rabbitmq'}
    };

    const expected = {
      traceId: 'a',
      parentId: 'b',
      id: 'c',
      name: 'next-message',
      timestamp: 1,
      duration: 1,
      annotations: [
        {value: 'wr', timestamp: 1, endpoint: {serviceName: 'backend'}},
        {value: 'mr', timestamp: 2, endpoint: {serviceName: 'backend'}},
      ],
      binaryAnnotations: [
        {
          key: 'ma',
          value: true,
          endpoint: {serviceName: 'rabbitmq'}
        }
      ]
    };

    const v1 = SPAN_V1.convert(v2);
    expect(v1).to.deep.equal(expected);
  });

  it('converts incomplete consumer span', () => {
    const v2 = {
      traceId: 'a',
      name: 'next-message',
      id: 'c',
      parentId: 'b',
      kind: 'CONSUMER',
      timestamp: 1,
      localEndpoint: {serviceName: 'backend'}
    };

    const expected = {
      traceId: 'a',
      parentId: 'b',
      id: 'c',
      name: 'next-message',
      timestamp: 1,
      annotations: [{value: 'mr', timestamp: 1, endpoint: {serviceName: 'backend'}}],
      binaryAnnotations: []
    };

    const v1 = SPAN_V1.convert(v2);
    expect(v1).to.deep.equal(expected);
  });

  it('converts local span', () => {
    const v2 = {
      traceId: 'a',
      name: 'process',
      id: 'c',
      parentId: 'b',
      timestamp: 1,
      localEndpoint: {serviceName: 'backend'}
    };

    const expected = {
      traceId: 'a',
      parentId: 'b',
      id: 'c',
      name: 'process',
      timestamp: 1,
      annotations: [],
      binaryAnnotations: [
        {key: 'lc', value: '', endpoint: {serviceName: 'backend'}}
      ]
    };

    const v1 = SPAN_V1.convert(v2);
    expect(v1).to.deep.equal(expected);
  });

  it('should write CS/CR when no annotations exist', () => {
    const v2 = {
      traceId: 'a',
      name: 'get',
      id: 'a',
      kind: 'CLIENT',
      timestamp: 1,
      duration: 2,
      localEndpoint: {
        serviceName: 'portalservice',
        ipv4: '10.57.50.83',
        port: 8080
      }
    };

    const v1 = SPAN_V1.convert(v2);
    expect(v1.annotations).to.deep.equal([
      {
        endpoint: {
          serviceName: 'portalservice',
          ipv4: '10.57.50.83',
          port: 8080
        },
        timestamp: 1,
        value: 'cs'
      },
      {
        endpoint: {
          serviceName: 'portalservice',
          ipv4: '10.57.50.83',
          port: 8080,
        },
        timestamp: 3,
        value: 'cr'
      }
    ]);
  });

  it('should maintain CS/CR annotation order', () => {
    const v2 = {
      traceId: 'a',
      name: 'get',
      id: 'a',
      kind: 'CLIENT',
      timestamp: 1,
      duration: 2,
      localEndpoint: {
        serviceName: 'portalservice',
        ipv4: '10.57.50.83',
        port: 8080
      },
      annotations: [
        {
          timestamp: 2,
          value: 'middle'
        }
      ]
    };

    const v1 = SPAN_V1.convert(v2);
    expect(v1.annotations.map(s => s.timestamp)).to.deep.equal([1, 2, 3]);
  });

  it('should set SA annotation on client span', () => {
    const v2 = {
      traceId: 'a',
      id: 'a',
      kind: 'CLIENT',
      remoteEndpoint: {
        serviceName: 'there',
        ipv4: '10.57.50.84',
        port: 80
      }
    };

    const v1 = SPAN_V1.convert(v2);
    expect(v1.binaryAnnotations).to.deep.equal([
      {
        key: 'sa',
        value: true,
        endpoint: {
          serviceName: 'there',
          ipv4: '10.57.50.84',
          port: 80
        }
      }
    ]);
  });

  it('should retain ipv4 and ipv6 addresses', () => {
    const localEndpoint = {
      serviceName: 'there',
      ipv4: '10.57.50.84',
      ipv6: '2001:db8::c001',
      port: 80
    };

    const v2 = {
      traceId: 'a',
      id: 'a',
      kind: 'CLIENT',
      timestamp: 1,
      localEndpoint
    };

    const v1 = SPAN_V1.convert(v2);
    expect(v1.annotations.map(s => s.endpoint)).to.deep.equal([localEndpoint]);
  });

  it('should backfill empty endpoint serviceName', () => {
    const v2 = {
      traceId: 'a',
      id: 'a',
      kind: 'CLIENT',
      timestamp: 1,
      localEndpoint: {
        ipv6: '2001:db8::c001'
      }
    };

    const v1 = SPAN_V1.convert(v2);
    expect(v1.annotations.map(s => s.endpoint)).to.deep.equal([{
      serviceName: '',
      ipv6: '2001:db8::c001'
    }]);
  });

  it('should not write timestamps for shared span', () => {
    const v2 = {
      traceId: 'a',
      name: 'get',
      id: 'a',
      kind: 'SERVER',
      shared: true,
      timestamp: 1,
      duration: 2,
      localEndpoint: {
        serviceName: 'portalservice',
        ipv4: '10.57.50.83',
        port: 8080
      }
    };

    const v1 = SPAN_V1.convert(v2);
    should.equal(v1.timestamp, undefined);
    should.equal(v1.duration, undefined);
  });
});

describe('SPAN v1 Merge', () => {
  const clientSpan = {
    traceId: 'a',
    name: 'get',
    id: 'c',
    parentId: 'b',
    annotations: [
      {
        endpoint: {
          serviceName: 'baloonservice',
          ipv4: '10.57.50.70',
          port: 80
        },
        timestamp: 1,
        value: 'cs'
      },
      {
        endpoint: {
          serviceName: 'baloonservice',
          ipv4: '10.57.50.70',
          port: 80,
        },
        timestamp: 4,
        value: 'cr'
      }
    ],
    binaryAnnotations: []
  };
  const serverSpan = {
    traceId: 'a',
    name: '',
    id: 'c',
    parentId: 'b',
    annotations: [
      {
        endpoint: {
          serviceName: 'portalservice',
          ipv4: '10.57.50.83',
          port: 8080
        },
        timestamp: 2,
        value: 'sr'
      },
      {
        endpoint: {
          serviceName: 'portalservice',
          ipv4: '10.57.50.83',
          port: 8080,
        },
        timestamp: 3,
        value: 'ss'
      }
    ],
    binaryAnnotations: []
  };
  const mergedSpan = {
    traceId: 'a',
    name: 'get',
    id: 'c',
    parentId: 'b',
    annotations: [
      {
        endpoint: {
          serviceName: 'baloonservice',
          ipv4: '10.57.50.70',
          port: 80
        },
        timestamp: 1,
        value: 'cs'
      },
      {
        endpoint: {
          serviceName: 'portalservice',
          ipv4: '10.57.50.83',
          port: 8080
        },
        timestamp: 2,
        value: 'sr'
      },
      {
        endpoint: {
          serviceName: 'portalservice',
          ipv4: '10.57.50.83',
          port: 8080,
        },
        timestamp: 3,
        value: 'ss'
      },
      {
        endpoint: {
          serviceName: 'baloonservice',
          ipv4: '10.57.50.70',
          port: 80,
        },
        timestamp: 4,
        value: 'cr'
      }
    ],
    binaryAnnotations: []
  };

  it('should merge server and client span', () => {
    const merged = SPAN_V1.merge(serverSpan, clientSpan);

    expect(merged).to.deep.equal(mergedSpan);
  });

  it('should merge client and server span', () => {
    const merged = SPAN_V1.merge(clientSpan, serverSpan);

    expect(merged).to.deep.equal(mergedSpan);
  });

  it('should overwrite client name with server name', () => {
    const merged = SPAN_V1.merge(clientSpan, {
      traceId: 'a',
      id: 'c',
      name: 'get /users/:userId',
      annotations: [{timestamp: 2, value: 'sr'}],
      binaryAnnotations: []
    });

    expect(merged.name).to.equal('get /users/:userId');
  });

  it('should not overwrite client name with empty', () => {
    const merged = SPAN_V1.merge(clientSpan, {
      traceId: 'a',
      id: 'c',
      name: '',
      annotations: [{timestamp: 2, value: 'sr'}],
      binaryAnnotations: []
    });

    expect(merged.name).to.equal(clientSpan.name);
  });

  it('should not overwrite client name with unknown', () => {
    const merged = SPAN_V1.merge(clientSpan, {
      traceId: 'a',
      id: 'c',
      name: 'unknown',
      annotations: [{timestamp: 2, value: 'sr'}],
      binaryAnnotations: []
    });

    expect(merged.name).to.equal(clientSpan.name);
  });
});
