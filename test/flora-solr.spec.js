'use strict';

const { expect } = require('chai');
const _ = require('lodash');
const nock = require('nock');
const errors = require('flora-errors');

const FloraSolr = require('../index');

const ImplementationError = errors.ImplementationError;

function escapeRegex(s) {
    return s.replace(/[-\/\\^$*+?.()|[\]{}]/g, '\\$&');
}

describe('Flora SOLR DataSource', () => {
    var solrUrl = 'http://example.com',
        solrIndexPath = '/solr/article/select',
        testResponse = '{"response":{"numFound":0,"docs":[]}}',
        dataSource,
        req;

    var api = {};

    beforeEach(() => {
        var cfg = {
                servers: {
                    default: { url: 'http://example.com/solr/' }
                }
            };

        dataSource = new FloraSolr(api, cfg);
    });

    afterEach(() => {
        if (req) req.done();
        nock.cleanAll();
    });

    after(() => {
        nock.restore();
    });

    describe('interface', () => {
        it('should export a query function', () => {
            expect(dataSource.process).to.be.a('function');
        });

        it('should export a prepare function', () => {
            expect(dataSource.prepare).to.be.a('function');
        });
    });

    it('should send requests using POST method', (done) => {
        req = nock(solrUrl)
            .post(solrIndexPath)
            .reply(200, testResponse);

        dataSource.process({ collection: 'article' }, done);
    });

    it('should set content type to "application/x-www-form-urlencoded"', (done) => {
        req = nock(solrUrl)
            .matchHeader('content-type', 'application/x-www-form-urlencoded')
            .post(solrIndexPath)
            .reply(200, testResponse);

        dataSource.process({ collection: 'article' }, done);
    });

    it('should set response format to JSON', (done) => {
        req = nock(solrUrl)
            .post(solrIndexPath, /wt=json/)
            .reply(200, testResponse);

        dataSource.process({ collection: 'article' }, done);
    });

    it('should use "default" if no explicit server is specified', (done) => {
        req = nock(solrUrl)
            .post(solrIndexPath)
            .reply(200, testResponse);

        dataSource.process({ collection: 'article' }, done);
    });

    it('should support multiple servers', (done) => {
        var archiveReq, awesomeReq, requests,
            floraRequests = [
                { collection: 'archive' },
                { server: 'other', collection: 'awesome-index' }
            ],
            ds = new FloraSolr(api, {
                servers: {
                    'default': { url: 'http://article.example.com/solr/' },
                    other: { url: 'http://other.example.com/solr/' }
                }
            });

        archiveReq = nock('http://article.example.com')
            .post('/solr/archive/select')
            .reply(200, testResponse);

        awesomeReq = nock('http://other.example.com')
            .post('/solr/awesome-index/select')
            .reply(200, testResponse);

        requests = floraRequests.map((request) => {
            return new Promise((resolve, reject) => {
                ds.process(request, (err, response) => {
                    if (err) return reject(err);
                    resolve(response);
                });
            });
        });

        // make sure requests are triggered
        // nock requests trigger an exception when done() is called and request to url was not triggered
        Promise.all(requests)
            .then(() => {
                archiveReq.done();
                awesomeReq.done();
                done();
            }, done)
            .catch(done);
    });

    describe('error handling', () => {
        it('should trigger error if status code >= 400', (done) => {
            req = nock(solrUrl)
                .post(solrIndexPath)
                .reply(500, '{}');

            dataSource.process({ collection: 'article' }, (err) => {
                expect(err).to.be.instanceof(Error);
                done();
            });
        });

        it('should trigger error if response cannot be parsed', (done) => {
            req = nock(solrUrl)
                .post(solrIndexPath)
                .reply(418, '<p>Something went wrong</p>');

            dataSource.process({ collection: 'article' }, (err) => {
                expect(err).to.be.instanceof(Error);
                done();
            });
        });

        it('should handle request\'s error event', (done) => {
            var cfg = {
                servers: {
                    'default': { url: 'http://doesnotexists.localhost/solr/' }
                }
            };

            // nock can't fake request errors at the moment, so we have to make a real request to nonexistent host
            dataSource = new FloraSolr(api, cfg);

            dataSource.process({ collection: 'article' }, (err) => {
                expect(err).to.be.instanceof(Error);
                done();
            });
        });

        it('should trigger an error for non-existent server', (done) => {
            dataSource.process({ server: 'non-existent', collection: 'article' }, (err) => {
                if (err) {
                    expect(err).to.be.instanceOf(Error);
                    expect(err.message).to.contain('Server "non-existent" not defined');
                    return done();
                }

                done(new Error('To access a non-existent server should trigger an error'));
            });
        });
    });

    describe('attributes', () => {
        it('should set requested attributes', (done) => {
            var request = { collection: 'article', attributes: ['id', 'name', 'date'] };

            req = nock(solrUrl)
                .post(solrIndexPath, /fl=id%2Cname%2Cdate/)
                .reply(200, testResponse);

            dataSource.process(request, done);
        });
    });

    describe('filters', () => {

        it('should send "*:*" if no filter is set', (done) => {
            req = nock(solrUrl)
                .post(solrIndexPath, /q=\*%3A\*/)
                .reply(200, testResponse);

            dataSource.process({ collection: 'article' }, done);
        });

        // test conversion of boolean values
        _({ false: 0, true: 1 }).forEach((conversionTarget, booleanValue) => {
            it('should transform boolean ' + booleanValue + ' value to ' + conversionTarget + '', (done) => {
                var request = {
                        collection: 'article',
                        filter: [
                            [{ attribute: 'foo', operator: 'equal', value: booleanValue !== 'false' }]
                        ]
                    },
                    paramRegex = new RegExp('q=\\(foo%3A' + conversionTarget + '\\)');

                req = nock(solrUrl)
                    .post(solrIndexPath, paramRegex)
                    .reply(200, testResponse);

                dataSource.process(request, done);
            });
        });

        it('should support arrays', (done) => {
            var request = {
                    collection: 'article',
                    filter: [
                        [{attribute: 'foo', operator: 'equal', value: [1, 3, 5, 7]}]
                    ]
                },
                paramRegex = new RegExp('q=\\(foo%3A\\(' + encodeURIComponent('1 OR 3 OR 5 OR 7') + '\\)');

            req = nock(solrUrl)
                .post(solrIndexPath, paramRegex)
                .reply(200, testResponse);

            dataSource.process(request, done);
        });

        var specialChars = [
            '\\', '/', '+', '-', '&', '|', '!', '(', ')',
            '{', '}', '[', ']', '^', '"', '~', '*', '?', ':'
        ];
        specialChars.forEach((character) => {
            it('should escape special character ' + character, (done) => {
                var request = {
                        collection: 'article',
                        filter: [
                            [{ attribute: 'foo', operator: 'equal', value: character + 'bar' }]
                        ]
                    },
                    encodedChar = encodeURIComponent(character),
                    paramRegex = new RegExp('q=\\(foo%3A%5C' + escapeRegex(encodedChar) + 'bar\\)');

                req = nock(solrUrl)
                    .post(solrIndexPath, paramRegex)
                    .reply(200, testResponse);

                dataSource.process(request, done);
            });
        });

        describe('range queries', () => {
            it('should support fixed lower boundaries', (done) => {
                var request = {
                        collection: 'article',
                        filter: [
                            [{ attribute: 'foo', operator: 'greaterOrEqual', value: 1337 }]
                        ]
                    },
                    encodedRangeQuery = encodeURIComponent('foo:[1337 TO *]'),
                    rangeQueryRegex = new RegExp('q=\\(' + escapeRegex(encodedRangeQuery) + '\\)');

                req = nock(solrUrl)
                    .post(solrIndexPath, rangeQueryRegex)
                    .reply(200, testResponse);

                dataSource.process(request, done);
            });

            it('should support fixed upper boundaries', (done) => {
                var request = {
                        collection: 'article',
                        filter: [
                            [{ attribute: 'foo', operator: 'lessOrEqual', value: 1337 }]
                        ]
                    },
                    encodedRangeQuery = encodeURIComponent('foo:[* TO 1337]'),
                    rangeQueryRegex = new RegExp('q=\\(' + escapeRegex(encodedRangeQuery) + '\\)');

                req = nock(solrUrl)
                    .post(solrIndexPath, rangeQueryRegex)
                    .reply(200, testResponse);

                dataSource.process(request, done);
            });

            it('should support fixed lower and upper boundaries', (done) => {
                var request = {
                        collection: 'article',
                        filter: [
                            [
                                { attribute: 'foo', operator: 'greaterOrEqual', value: 1337 },
                                { attribute: 'foo', operator: 'lessOrEqual', value: 4711 }
                            ]
                        ]
                    },
                    encodedRangeQuery = encodeURIComponent('foo:[1337 TO 4711]'),
                    rangeQueryRegex = new RegExp('q=\\(' + escapeRegex(encodedRangeQuery) + '\\)');

                req = nock(solrUrl)
                    .post(solrIndexPath, rangeQueryRegex)
                    .reply(200, testResponse);

                dataSource.process(request, done);
            });
        });

        it('should transform single filters', (done) => {
            var request = {
                    collection: 'article',
                    filter: [
                        [{ attribute: 'foo', operator: 'equal', value: 'foo' }]
                    ]
                };

            req = nock(solrUrl)
                .post(solrIndexPath, /q=\(foo%3Afoo\)/)
                .reply(200, testResponse);

            dataSource.process(request, done);
        });

        it('should transform complex filters', (done) => {
            var request = {
                    collection: 'article',
                    search: 'foo bar',
                    filter: [
                        [
                            { attribute: 'authorId', operator: 'equal', value: 1337 },
                            { attribute: 'typeId', operator: 'equal', value: 4711 }
                        ],
                        [{ attribute: 'status', operator: 'equal', value: 'future' }]
                    ]
                },
                paramRegex = /q=foo%20bar%20AND%20\(\(authorId%3A1337%20AND%20typeId%3A4711\)%20OR%20\(status%3Afuture\)\)/;

            req = nock(solrUrl)
                .post(solrIndexPath, paramRegex)
                .reply(200, testResponse);

            dataSource.process(request, done);
        });

        it('should support composite key filters', (done) => {
            var request = {
                    collection: 'awesome_index',
                    filter: [
                        [
                            {
                                attribute: ['intKey', 'stringKey', 'boolKey'],
                                operator: 'equal',
                                // test string escaping and boolean conversion
                                value: [[1337, '(foo)', true], [4711, 'bar!', false]]
                            }
                        ]
                    ]
                },
                solrQuery,
                paramRegex;

            solrQuery = '((intKey:1337 AND stringKey:\\(foo\\) AND boolKey:1)';
            solrQuery += ' OR ';
            solrQuery += '(intKey:4711 AND stringKey:bar\\! AND boolKey:0))';
            paramRegex = new RegExp('q=' + encodeURIComponent(solrQuery).replace(/(\(|\))/g, '\\$1'));

            req = nock(solrUrl)
                .post('/solr/awesome_index/select', paramRegex)
                .reply(200, testResponse);

            dataSource.process(request, done);
        });

        var supportedFilters = {
            equal: /date%3A2015%5C-12%5C-31/,
            greaterOrEqual: /date%3A%5B2015%5C-12%5C-31%20TO%20\*%5D/,
            lessOrEqual: /date%3A%5B\*%20TO%202015%5C-12%5C-31%5D/,
            notEqual: /\-date%3A2015%5C-12%5C-31/
        };
        Object.keys(supportedFilters).forEach((operator) => {
            it('should support "' + operator + '" filters', (done) => {
                var paramRegEx = supportedFilters[operator],
                    request = {
                        collection: 'article',
                        filter: [
                            [{attribute: 'date', operator: operator, value: '2015-12-31'}]
                        ]
                    };

                req = nock(solrUrl)
                    .post('/solr/article/select', paramRegEx)
                    .reply(200, testResponse);

                dataSource.process(request, done);
            });
        });

        ['less', 'greater'].forEach((operator) => {
            it('should trigger an error for unsupported filter operator "' + operator + '"', (done) => {
                var request = {
                    collection: 'article',
                    filter: [
                        [{attribute: 'date', operator: operator, value: '2015-12-31'}]
                    ]
                };

                dataSource.process(request, (err) => {
                    expect(err).to.be.instanceOf(ImplementationError);
                    expect(err.message).to.contain('not support "' + operator + '" filters');
                    done();
                });
            });
        });

        it('should append additional query parameters', (done) => {
            var request = {
                collection: 'awesome_index',
                queryAddition: "_val_:\"product(assetClassBoost,3)\"\n_val_:\"product(importance,50)\""
            };

            req = nock(solrUrl)
                .post('/solr/awesome_index/select', /q=_val_%3A%22product\(assetClassBoost%2C3\)%22%20_val_%3A%22product\(importance%2C50\)%22/)
                .reply(200, testResponse);

            dataSource.process(request, done);
        });
    });

    describe('full-text search', () => {
        it('should add search term to query', (done) => {
            var request = {
                collection: 'article',
                search: 'fo(o)bar'
            };

            req = nock(solrUrl)
                .post(solrIndexPath, /q=fo%5C\(o%5C\)bar/)
                .reply(200, testResponse);

            dataSource.process(request, done);
        });

        it('should support additional filter(s)', (done) => {
            var request = {
                collection: 'article',
                search: 'foo bar',
                filter: [
                    [{ attribute: 'authorId', operator: 'equal', value: 1337 }]
                ]
            };

            req = nock(solrUrl)
                .post(solrIndexPath, /q=foo%20bar%20AND%20\(authorId%3A1337\)/)
                .reply(200, testResponse);

            dataSource.process(request, done);
        });
    });

    describe('order', () => {
        it('single criterion', (done) => {
            var request = {
                collection: 'article',
                order: [{ attribute: 'foo', direction: 'asc' }]
            };

            req = nock(solrUrl)
                .post(solrIndexPath, /sort=foo%20asc/)
                .reply(200, testResponse);

            dataSource.process(request, done);
        });

        it('multiple criteria', (done) => {
            var request = {
                collection: 'article',
                order: [
                    { attribute: 'foo', direction: 'asc' },
                    { attribute: 'bar', direction: 'desc' }
                ]
            };

            req = nock(solrUrl)
                .post(solrIndexPath, /sort=foo%20asc%2Cbar%20desc/)
                .reply(200, testResponse);

            dataSource.process(request, done);
        });
    });

    describe('pagination', () => {
        it('should set limit', (done) => {
            req = nock(solrUrl)
                .post(solrIndexPath, /rows=15/)
                .reply(200, testResponse);

            dataSource.process({ collection: 'article', limit: 15 }, done);
        });

        it('should overwrite SOLR default limit for sub-resource processing', (done) => {
            req = nock(solrUrl)
                .post(solrIndexPath, /rows=1000000/)
                .reply(200, testResponse);

            // no explicit limit set
            dataSource.process({ collection: 'article' }, done);
        });

        it('should set page', (done) => {
            req = nock(solrUrl)
                // only return sorted pagination params because they can appear in any order
                .filteringRequestBody((body) => {
                    var params = require('querystring').parse(body),
                        paginationParams = [];

                    ['rows', 'start'].forEach((key) => { // extract pagination params
                        if (params[key]) {
                            paginationParams.push(key + '=' + params[key]);
                        }
                    });

                    return paginationParams.sort().join('&');
                })
                .post(solrIndexPath, /rows=10&start=20/)
                .reply(200, testResponse);

            dataSource.process({ collection: 'article', limit: 10, page: 3 }, done);
        });
    });

    describe('limitPer', () => {
        it('should activate result grouping', (done) => {
            req = nock(solrUrl)
                .post(solrIndexPath, /group=true/)
                .reply(200, testResponse);

            dataSource.process({ collection: 'article', limitPer: 'seriesId' }, done);
        });

        it('should use limitPer as group.field parameter', (done) => {
            req = nock(solrUrl)
                .post(solrIndexPath, /group\.field=seriesId/)
                .reply(200, testResponse);

            dataSource.process({ collection: 'article', limitPer: 'seriesId' }, done);
        });

        it('should return flat list instead of groups', (done) => {
            req = nock(solrUrl)
                .post(solrIndexPath, (body) => {
                    return body['group.format'] === 'simple'
                        && body['group.main'] === 'true';
                })
                .reply(200, testResponse);

            dataSource.process({ collection: 'article', limitPer: 'seriesId'}, done);
        });

        it('should set limit', (done) => {
            req = nock(solrUrl)
                .post(solrIndexPath, (body) => {
                    return body['group.limit'] == 3
                        && body['rows'] == 1000000;
                })
                .reply(200, testResponse);

            dataSource.process({ collection: 'article', limitPer: 'seriesId', limit: 3}, done);
        });

        it('should not set group sort order', (done) => {
            req = nock(solrUrl)
                .post(solrIndexPath, (body) => {
                    return !body['group.sort'];
                })
                .reply(200, testResponse);

            dataSource.process({
                collection: 'article',
                limitPer: 'seriesId',
                order: [{ attribute: 'date', direction: 'desc' }]
            }, done);
        });
    });
});
