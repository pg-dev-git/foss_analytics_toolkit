"""Checked-in Salesforce Wave API response fixtures.

Every payload in this module was captured from a live Salesforce org and then
sanitized: key names, nesting and value *shapes* are exactly what the platform
returns, while every org-specific value (ids, user names, instance URL) was
replaced with a synthetic placeholder.

These fixtures exist because the Pydantic models used to be tested against
invented payloads that happened to match the implementation's assumptions
rather than the platform's. Three Critical/High defects (list-orgs returning
nothing, dataset/dashboard listing truncating to page 1, and dataflow jobs
failing to parse) were invisible to the test suite for exactly that reason.

When a Salesforce response model changes, update the matching fixture here
rather than writing a new ad-hoc dict in a test.

Endpoint reference for each fixture is cited above the constant.
"""

# GET /services/data/{version}/wave/datasets?pageSize=5
# Page 1 of a paginated dataset listing.
#
# Carries a `nextPageUrl` whose query parameter is `page` (base64), NOT
# `pageToken`. The platform silently ignores an unknown `pageToken` and
# returns page 1 again.
DATASETS_PAGE_1 = {
    'datasets': [
        {
            'clientShardsUrl': '/services/data/v67.0/wave/datasets/0Fb010000000000000/shards',
            'createdBy': {
                'id': '005020000000000AAA',
                'name': 'Example User',
                'profilePhotoUrl': 'https://example-org.my.salesforce.com/profilephoto/005/T',
            },
            'createdDate': '2026-07-15T20:12:55.000Z',
            'currentVersionId': '0Fc030000000000000',
            'currentVersionUrl': '/services/data/v67.0/wave/datasets/0Fb010000000000000/versions/0Fc030000000000000',
            'dataRefreshDate': '2026-09-20T02:42:32.000Z',
            'datasetType': 'default',
            'folder': {
                'id': '00l040000000000AAA',
                'label': 'salestest',
                'name': 'salestest',
                'url': '/services/data/v67.0/wave/folders/00l040000000000AAA',
            },
            'id': '0Fb010000000000000',
            'label': 'Accounts',
            'lastAccessedDate': '2026-10-21T13:26:51.000Z',
            'lastModifiedBy': {
                'id': '005050000000000AAA',
                'name': 'Example Integration User',
                'profilePhotoUrl': 'https://example-org.my.salesforce.com/profilephoto/005/T',
            },
            'lastModifiedDate': '2026-09-20T02:42:36.000Z',
            'lastQueriedDate': '2026-10-05T20:39:49.000Z',
            'name': 'account',
            'permissions': {
                'create': True,
                'manage': True,
                'modify': True,
                'view': True,
            },
            'type': 'dataset',
            'url': '/services/data/v67.0/wave/datasets/0Fb010000000000000',
            'versionsUrl': '/services/data/v67.0/wave/datasets/0Fb010000000000000/versions',
            'visibility': 'All',
        },
        {
            'clientShardsUrl': '/services/data/v67.0/wave/datasets/0Fb060000000000000/shards',
            'createdBy': {
                'id': '005020000000000AAA',
                'name': 'Example User',
                'profilePhotoUrl': 'https://example-org.my.salesforce.com/profilephoto/005/T',
            },
            'createdDate': '2026-07-15T20:12:55.000Z',
            'currentVersionId': '0Fc070000000000000',
            'currentVersionUrl': '/services/data/v67.0/wave/datasets/0Fb060000000000000/versions/0Fc070000000000000',
            'dataRefreshDate': '2026-09-20T02:42:32.000Z',
            'datasetType': 'default',
            'folder': {
                'id': '00l040000000000AAA',
                'label': 'salestest',
                'name': 'salestest',
                'url': '/services/data/v67.0/wave/folders/00l040000000000AAA',
            },
            'id': '0Fb060000000000000',
            'label': 'Activities',
            'lastAccessedDate': '2026-10-05T20:39:49.000Z',
            'lastModifiedBy': {
                'id': '005050000000000AAA',
                'name': 'Example Integration User',
                'profilePhotoUrl': 'https://example-org.my.salesforce.com/profilephoto/005/T',
            },
            'lastModifiedDate': '2026-09-20T02:42:36.000Z',
            'lastQueriedDate': '2026-10-05T20:39:49.000Z',
            'name': 'activity1',
            'permissions': {
                'create': True,
                'manage': True,
                'modify': True,
                'view': True,
            },
            'type': 'dataset',
            'url': '/services/data/v67.0/wave/datasets/0Fb060000000000000',
            'versionsUrl': '/services/data/v67.0/wave/datasets/0Fb060000000000000/versions',
            'visibility': 'All',
        },
    ],
    'nextPageUrl': '/services/data/v67.0/wave/datasets?page=eyJwYWdlU2l6ZSI6NSwibGFzdElkIjoiMEZiYWswMDAwMDJIajNIQ0FTIiwibGFzdE5hbWUiOiJGdW5kcmFpc2luZyBPcHBvcnR1bml0aWVzIiwibGFzdFRpbWUiOjE3OTEyMzI3ODkwMDAsIm90aGVyUGFyYW1ldGVycyI6eyJoYXNDdXJyZW50T25seSI6ImZhbHNlIiwiaW5jbHVkZUN1cnJlbnRWZXJzaW9uIjoiZmFsc2UifSwiZGF0YXNldFNvcnRPcmRlciI6Ik1SVSJ9',
    'totalSize': 19,
    'url': '/services/data/v67.0/wave/datasets?pageSize=5&sort=Mru',
}

# GET /services/data/{version}/wave/datasets?pageSize=5&page=<token>
# Page 2, reached by following the `nextPageUrl` from page 1.
DATASETS_PAGE_2 = {
    'datasets': [
        {
            'clientShardsUrl': '/services/data/v67.0/wave/datasets/0Fb210000000000000/shards',
            'createdBy': {
                'id': '005020000000000AAA',
                'name': 'Example User',
                'profilePhotoUrl': 'https://example-org.my.salesforce.com/profilephoto/005/T',
            },
            'createdDate': '2026-07-16T13:56:35.000Z',
            'currentVersionId': '0Fc220000000000000',
            'currentVersionUrl': '/services/data/v67.0/wave/datasets/0Fb210000000000000/versions/0Fc220000000000000',
            'dataRefreshDate': '2026-07-16T13:56:47.000Z',
            'datasetType': 'default',
            'folder': {
                'id': '00l040000000000AAA',
                'label': 'salestest',
                'name': 'salestest',
                'url': '/services/data/v67.0/wave/folders/00l040000000000AAA',
            },
            'id': '0Fb210000000000000',
            'label': 'Report',
            'lastAccessedDate': '2026-10-05T20:39:49.000Z',
            'lastModifiedBy': {
                'id': '005050000000000AAA',
                'name': 'Example Integration User',
                'profilePhotoUrl': 'https://example-org.my.salesforce.com/profilephoto/005/T',
            },
            'lastModifiedDate': '2026-07-16T13:56:47.000Z',
            'licenseAttributes': {
                'type': 'einsteinanalytics',
            },
            'name': 'Report',
            'permissions': {
                'create': True,
                'manage': True,
                'modify': True,
                'view': True,
            },
            'type': 'dataset',
            'url': '/services/data/v67.0/wave/datasets/0Fb210000000000000',
            'versionsUrl': '/services/data/v67.0/wave/datasets/0Fb210000000000000/versions',
            'visibility': 'All',
        },
        {
            'clientShardsUrl': '/services/data/v67.0/wave/datasets/0Fb230000000000000/shards',
            'createdBy': {
                'id': '005020000000000AAA',
                'name': 'Example User',
                'profilePhotoUrl': 'https://example-org.my.salesforce.com/profilephoto/005/T',
            },
            'createdDate': '2026-07-15T20:12:55.000Z',
            'currentVersionId': '0Fc240000000000000',
            'currentVersionUrl': '/services/data/v67.0/wave/datasets/0Fb230000000000000/versions/0Fc240000000000000',
            'dataRefreshDate': '2026-09-20T02:42:32.000Z',
            'datasetType': 'default',
            'folder': {
                'id': '00l040000000000AAA',
                'label': 'salestest',
                'name': 'salestest',
                'url': '/services/data/v67.0/wave/folders/00l040000000000AAA',
            },
            'id': '0Fb230000000000000',
            'label': 'User Allocation',
            'lastAccessedDate': '2026-10-05T20:39:49.000Z',
            'lastModifiedBy': {
                'id': '005050000000000AAA',
                'name': 'Example Integration User',
                'profilePhotoUrl': 'https://example-org.my.salesforce.com/profilephoto/005/T',
            },
            'lastModifiedDate': '2026-09-20T02:42:36.000Z',
            'lastQueriedDate': '2026-10-05T20:39:49.000Z',
            'name': 'quota',
            'permissions': {
                'create': True,
                'manage': True,
                'modify': True,
                'view': True,
            },
            'type': 'dataset',
            'url': '/services/data/v67.0/wave/datasets/0Fb230000000000000',
            'versionsUrl': '/services/data/v67.0/wave/datasets/0Fb230000000000000/versions',
            'visibility': 'All',
        },
    ],
    'nextPageUrl': '/services/data/v67.0/wave/datasets?page=eyJwYWdlU2l6ZSI6NSwibGFzdElkIjoiMEZiYWswMDAwMDJIajJzQ0FDIiwibGFzdE5hbWUiOiJDYXNlcyIsImxhc3RUaW1lIjoxNzkxMjMyNzg4MDAwLCJvdGhlclBhcmFtZXRlcnMiOnsiaGFzQ3VycmVudE9ubHkiOiJmYWxzZSIsImluY2x1ZGVDdXJyZW50VmVyc2lvbiI6ImZhbHNlIn0sImRhdGFzZXRTb3J0T3JkZXIiOiJNUlUifQ%3D%3D',
    'totalSize': 19,
    'url': '/services/data/v67.0/wave/datasets?page=eyJwYWdlU2l6ZSI6NSwibGFzdElkIjoiMEZiYWswMDAwMDJIajNIQ0FTIiwibGFzdE5hbWUiOiJGdW5kcmFpc2luZyBPcHBvcnR1bml0aWVzIiwibGFzdFRpbWUiOjE3OTEyMzI3ODkwMDAsIm90aGVyUGFyYW1ldGVycyI6eyJoYXNDdXJyZW50T25seSI6ImZhbHNlIiwiaW5jbHVkZUN1cnJlbnRWZXJzaW9uIjoiZmFsc2UifSwiZGF0YXNldFNvcnRPcmRlciI6Ik1SVSJ9',
}

# GET /services/data/{version}/wave/datasets
# Full dataset listing for a small org (no next page).
DATASETS = {
    'datasets': [
        {
            'clientShardsUrl': '/services/data/v67.0/wave/datasets/0Fb010000000000000/shards',
            'createdBy': {
                'id': '005020000000000AAA',
                'name': 'Example User',
                'profilePhotoUrl': 'https://example-org.my.salesforce.com/profilephoto/005/T',
            },
            'createdDate': '2026-07-15T20:12:55.000Z',
            'currentVersionId': '0Fc030000000000000',
            'currentVersionUrl': '/services/data/v67.0/wave/datasets/0Fb010000000000000/versions/0Fc030000000000000',
            'dataRefreshDate': '2026-09-20T02:42:32.000Z',
            'datasetType': 'default',
            'folder': {
                'id': '00l040000000000AAA',
                'label': 'salestest',
                'name': 'salestest',
                'url': '/services/data/v67.0/wave/folders/00l040000000000AAA',
            },
            'id': '0Fb010000000000000',
            'label': 'Accounts',
            'lastAccessedDate': '2026-10-21T13:26:51.000Z',
            'lastModifiedBy': {
                'id': '005050000000000AAA',
                'name': 'Example Integration User',
                'profilePhotoUrl': 'https://example-org.my.salesforce.com/profilephoto/005/T',
            },
            'lastModifiedDate': '2026-09-20T02:42:36.000Z',
            'lastQueriedDate': '2026-10-05T20:39:49.000Z',
            'name': 'account',
            'permissions': {
                'create': True,
                'manage': True,
                'modify': True,
                'view': True,
            },
            'type': 'dataset',
            'url': '/services/data/v67.0/wave/datasets/0Fb010000000000000',
            'versionsUrl': '/services/data/v67.0/wave/datasets/0Fb010000000000000/versions',
            'visibility': 'All',
        },
        {
            'clientShardsUrl': '/services/data/v67.0/wave/datasets/0Fb060000000000000/shards',
            'createdBy': {
                'id': '005020000000000AAA',
                'name': 'Example User',
                'profilePhotoUrl': 'https://example-org.my.salesforce.com/profilephoto/005/T',
            },
            'createdDate': '2026-07-15T20:12:55.000Z',
            'currentVersionId': '0Fc070000000000000',
            'currentVersionUrl': '/services/data/v67.0/wave/datasets/0Fb060000000000000/versions/0Fc070000000000000',
            'dataRefreshDate': '2026-09-20T02:42:32.000Z',
            'datasetType': 'default',
            'folder': {
                'id': '00l040000000000AAA',
                'label': 'salestest',
                'name': 'salestest',
                'url': '/services/data/v67.0/wave/folders/00l040000000000AAA',
            },
            'id': '0Fb060000000000000',
            'label': 'Activities',
            'lastAccessedDate': '2026-10-05T20:39:49.000Z',
            'lastModifiedBy': {
                'id': '005050000000000AAA',
                'name': 'Example Integration User',
                'profilePhotoUrl': 'https://example-org.my.salesforce.com/profilephoto/005/T',
            },
            'lastModifiedDate': '2026-09-20T02:42:36.000Z',
            'lastQueriedDate': '2026-10-05T20:39:49.000Z',
            'name': 'activity1',
            'permissions': {
                'create': True,
                'manage': True,
                'modify': True,
                'view': True,
            },
            'type': 'dataset',
            'url': '/services/data/v67.0/wave/datasets/0Fb060000000000000',
            'versionsUrl': '/services/data/v67.0/wave/datasets/0Fb060000000000000/versions',
            'visibility': 'All',
        },
    ],
    'nextPageUrl': None,
    'totalSize': 19,
    'url': '/services/data/v67.0/wave/datasets',
}

# GET /services/data/{version}/wave/dashboards
# Dashboard listing. Uses the same `nextPageUrl` / `page` convention as
# datasets.
DASHBOARDS = {
    'dashboards': [
        {
            'allowPreview': True,
            'assetSharingUrl': 'https://example-org.my.salesforce.com/analytics/wave/dashboard?assetId=0FK080000000000000&orgId=00D090000000000AAA&loginHost=example.my.salesforce.com&urlType=sharing',
            'createdBy': {
                'id': '005020000000000AAA',
                'name': 'Example User',
                'profilePhotoUrl': 'https://example-org.my.salesforce.com/profilephoto/005/T',
            },
            'createdDate': '2026-07-15T20:12:55.000Z',
            'datasets': [
                {
                    'id': '0Fb010000000000000',
                    'label': 'Accounts',
                    'name': 'account',
                    'url': '/services/data/v67.0/wave/datasets/0Fb010000000000000',
                },
                {
                    'id': '0Fb100000000000000',
                    'label': 'Oppty Products',
                    'name': 'opportunity_products',
                    'url': '/services/data/v67.0/wave/datasets/0Fb100000000000000',
                },
                {
                    'id': '0Fb110000000000000',
                    'label': 'Products',
                    'name': 'product',
                    'url': '/services/data/v67.0/wave/datasets/0Fb110000000000000',
                },
            ],
            'dateVersion': 1,
            'files': [],
            'folder': {
                'id': '00l040000000000AAA',
                'label': 'salestest',
                'name': 'salestest',
                'url': '/services/data/v67.0/wave/folders/00l040000000000AAA',
            },
            'historiesUrl': '/services/data/v67.0/wave/dashboards/0FK080000000000000/histories',
            'id': '0FK080000000000000',
            'label': 'Sales Rep Whitespace',
            'lastAccessedDate': '2026-10-05T20:39:44.000Z',
            'lastModifiedBy': {
                'id': '005020000000000AAA',
                'name': 'Example User',
                'profilePhotoUrl': 'https://example-org.my.salesforce.com/profilephoto/005/T',
            },
            'lastModifiedDate': '2026-07-15T20:12:55.000Z',
            'mobileDisabled': False,
            'name': 'Opportunity_Discovery',
            'permissions': {
                'create': True,
                'manage': True,
                'modify': True,
                'view': True,
            },
            'refreshDate': '2026-09-20T02:42:03.000Z',
            'templateAssetSourceName': 'Opportunity_Discovery',
            'templateSourceId': 'sfdc_internal__Sales_Analytics_Flex',
            'type': 'dashboard',
            'url': '/services/data/v67.0/wave/dashboards/0FK080000000000000',
            'visibility': 'All',
        },
        {
            'allowPreview': True,
            'assetSharingUrl': 'https://example-org.my.salesforce.com/analytics/wave/dashboard?assetId=0FK120000000000000&orgId=00D090000000000AAA&loginHost=example.my.salesforce.com&urlType=sharing',
            'createdBy': {
                'id': '005020000000000AAA',
                'name': 'Example User',
                'profilePhotoUrl': 'https://example-org.my.salesforce.com/profilephoto/005/T',
            },
            'createdDate': '2026-07-15T20:12:55.000Z',
            'datasets': [
                {
                    'id': '0Fb130000000000000',
                    'label': 'Opportunities',
                    'name': 'opportunity',
                    'url': '/services/data/v67.0/wave/datasets/0Fb130000000000000',
                },
                {
                    'id': '0Fb140000000000000',
                    'label': 'Pipeline Trending',
                    'name': 'pipeline_trending',
                    'url': '/services/data/v67.0/wave/datasets/0Fb140000000000000',
                },
            ],
            'dateVersion': 1,
            'files': [
                {
                    'contentType': 'image/png',
                    'fileLength': 15791,
                    'fileName': 'assetPreviewThumb',
                    'id': '0FJak000000OqoiGAC',
                    'lastModifiedDate': '2026-07-15T20:12:55.000Z',
                    'url': '/services/data/v67.0/wave/lenses/0FK120000000000000/files/0FJak000000OqoiGAC',
                },
            ],
            'folder': {
                'id': '00l040000000000AAA',
                'label': 'salestest',
                'name': 'salestest',
                'url': '/services/data/v67.0/wave/folders/00l040000000000AAA',
            },
            'historiesUrl': '/services/data/v67.0/wave/dashboards/0FK120000000000000/histories',
            'id': '0FK120000000000000',
            'label': 'Sales Stage Analysis',
            'lastAccessedDate': '2026-10-05T20:39:44.000Z',
            'lastModifiedBy': {
                'id': '005020000000000AAA',
                'name': 'Example User',
                'profilePhotoUrl': 'https://example-org.my.salesforce.com/profilephoto/005/T',
            },
            'lastModifiedDate': '2026-07-15T20:12:55.000Z',
            'mobileDisabled': False,
            'name': 'Sales_Stage_Analysis',
            'permissions': {
                'create': True,
                'manage': True,
                'modify': True,
                'view': True,
            },
            'refreshDate': '2026-09-20T02:41:42.000Z',
            'templateAssetSourceName': 'Sales_Stage_Analysis',
            'templateSourceId': 'sfdc_internal__Sales_Analytics_Flex',
            'type': 'dashboard',
            'url': '/services/data/v67.0/wave/dashboards/0FK120000000000000',
            'visibility': 'All',
        },
    ],
    'nextPageUrl': '/services/data/v67.0/wave/dashboards?page=eyJwYWdlU2l6ZSI6MjUsInNvcnRPcmRlciI6Ik1SVSIsImxhc3RJZCI6IjBGS2FrMDAwMDAxOHpkakdBQSIsImxhc3ROYW1lIjoiVGVhbSBUcmVuZGluZyIsImxhc3RUaW1lIjoxNzkxMjMyNzgyMDAwfQ%3D%3D',
    'totalSize': 27,
    'url': '/services/data/v67.0/wave/dashboards',
}

# GET /services/data/{version}/wave/dataflows
# Dataflow listing. The response has no `nextPageUrl` key at all.
DATAFLOWS = {
    'dataflows': [
        {
            'createdBy': {
                'id': '005020000000000AAA',
                'name': 'Example User',
                'profilePhotoUrl': '/img/userprofile/default_profile_45_v2.png',
            },
            'createdDate': '2026-07-15T20:12:55.000Z',
            'description': 'Dataflow for the Sales Analytics Application.',
            'emailNotificationLevel': 'never',
            'historiesUrl': '/services/data/v67.0/wave/dataflows/02K150000000000AAA/histories',
            'id': '02K150000000000AAA',
            'label': 'salestest eltDataflow',
            'lastModifiedBy': {
                'id': '005020000000000AAA',
                'name': 'Example User',
                'profilePhotoUrl': '/img/userprofile/default_profile_45_v2.png',
            },
            'lastModifiedDate': '2026-07-15T20:12:55.000Z',
            'name': 'salestest_eltDataflow',
            'type': 'dataflow',
            'url': '/services/data/v67.0/wave/dataflows/02K150000000000AAA',
        },
        {
            'createdBy': {
                'id': '005020000000000AAA',
                'name': 'Example User',
                'profilePhotoUrl': '/img/userprofile/default_profile_45_v2.png',
            },
            'createdDate': '2026-07-15T20:12:55.000Z',
            'description': 'The_Motivator',
            'emailNotificationLevel': 'warnings',
            'historiesUrl': '/services/data/v67.0/wave/dataflows/02K160000000000AAA/histories',
            'id': '02K160000000000AAA',
            'label': 'The_Motivator',
            'lastModifiedBy': {
                'id': '005020000000000AAA',
                'name': 'Example User',
                'profilePhotoUrl': '/img/userprofile/default_profile_45_v2.png',
            },
            'lastModifiedDate': '2026-07-15T20:12:55.000Z',
            'name': 'The_Motivator',
            'type': 'dataflow',
            'url': '/services/data/v67.0/wave/dataflows/02K160000000000AAA',
        },
    ],
}

# GET /services/data/{version}/wave/dataflowjobs
# Dataflow job listing.
#
# IMPORTANT: the platform does NOT return `dataflowId`, `dataflowName`,
# `command`, `startTime` or `endTime` on a job. A job carries `id`,
# `label`, `status`, `jobType`, `startDate`, `executedDate`, `duration`,
# `progress`, `retryCount`, `waitTime`, `message` and `syncDataflows`.
# `startDate`/`executedDate` are absent while the job is still queued.
DATAFLOW_JOBS = {
    'dataflowJobs': [
        {
            'createdDate': '2026-10-21T13:52:01.000Z',
            'id': '030170000000000AAA',
            'jobType': 'datasync',
            'label': 'Event (Replication)',
            'nodesUrl': '/services/data/v67.0/wave/dataflowjobs/030170000000000AAA/nodes',
            'progress': 0.0,
            'retryCount': 0,
            'startDate': '2026-10-21T13:52:01.000Z',
            'status': 'Queued',
            'syncDataflows': {
                'dataflows': [],
            },
            'type': 'dataflowjob',
            'url': '/services/data/v67.0/wave/dataflowjobs/030170000000000AAA',
            'waitTime': 147,
        },
        {
            'createdDate': '2026-10-21T13:52:00.000Z',
            'executedDate': '2026-10-21T13:54:27.000Z',
            'id': '03C180000000000AAA',
            'jobType': 'datasync',
            'label': 'Opportunity (SFDC_LOCAL)',
            'nodesUrl': '/services/data/v67.0/wave/dataflowjobs/03C180000000000AAA/nodes',
            'progress': 0.0,
            'retryCount': 0,
            'startDate': '2026-10-21T13:52:00.000Z',
            'status': 'Running',
            'syncDataflows': {
                'dataflows': [],
            },
            'type': 'dataflowjob',
            'url': '/services/data/v67.0/wave/dataflowjobs/03C180000000000AAA',
            'waitTime': 147,
        },
    ],
    'nextPageUrl': None,
    'url': '/services/data/v67.0/wave/dataflowjobs',
}

# GET /services/data/{version}/wave/lenses
# Lens listing. Shares the dashboard payload shape.
LENSES = {
    'lenses': [
        {
            'allowPreview': True,
            'assetSharingUrl': 'https://example-org.my.salesforce.com/analytics/wave/lens?assetId=0FK190000000000000&orgId=00D090000000000AAA&loginHost=example.my.salesforce.com&urlType=sharing',
            'createdBy': {
                'id': '005020000000000AAA',
                'name': 'Example User',
                'profilePhotoUrl': 'https://example-org.my.salesforce.com/profilephoto/005/T',
            },
            'createdDate': '2026-07-15T20:12:55.000Z',
            'datasets': [
                {
                    'id': '0Fb130000000000000',
                    'name': 'opportunity',
                    'url': '/services/data/v67.0/wave/datasets/0Fb130000000000000',
                },
            ],
            'dateVersion': 1,
            'description': 'Get actionable insights from your opportunities data, starting with pushed opportunities. Narrow the time period to look at pushed opportunities for a year, quarter, month, and so on. Select a new grouping, such as by team or by opportunity owner. Or filter by a different stage, status, product, or any other way you break down opportunities.',
            'files': [],
            'filesUrl': '/services/data/v67.0/wave/lenses/0FK190000000000000/files',
            'folder': {
                'id': '00l040000000000AAA',
                'label': 'salestest',
                'name': 'salestest',
                'url': '/services/data/v67.0/wave/folders/00l040000000000AAA',
            },
            'historiesUrl': '/services/data/v67.0/wave/lenses/0FK190000000000000/histories',
            'id': '0FK190000000000000',
            'label': 'Explore Opportunities',
            'lastAccessedDate': '2026-10-05T20:39:46.000Z',
            'lastModifiedBy': {
                'id': '005020000000000AAA',
                'name': 'Example User',
                'profilePhotoUrl': 'https://example-org.my.salesforce.com/profilephoto/005/T',
            },
            'lastModifiedDate': '2026-07-15T20:12:55.000Z',
            'name': 'Explore_Opportunities',
            'permissions': {
                'create': True,
                'manage': True,
                'modify': True,
                'view': True,
            },
            'refreshDate': '2026-09-20T02:41:28.000Z',
            'templateAssetSourceName': 'Explore_Opportunities',
            'templateSourceId': 'sfdc_internal__Sales_Analytics_Flex',
            'type': 'lens',
            'url': '/services/data/v67.0/wave/lenses/0FK190000000000000',
            'visibility': 'All',
            'visualizationType': 'pyramid',
        },
        {
            'allowPreview': True,
            'assetSharingUrl': 'https://example-org.my.salesforce.com/analytics/wave/lens?assetId=0FK200000000000000&orgId=00D090000000000AAA&loginHost=example.my.salesforce.com&urlType=sharing',
            'createdBy': {
                'id': '005020000000000AAA',
                'name': 'Example User',
                'profilePhotoUrl': 'https://example-org.my.salesforce.com/profilephoto/005/T',
            },
            'createdDate': '2026-07-15T20:12:55.000Z',
            'datasets': [
                {
                    'id': '0Fb140000000000000',
                    'name': 'pipeline_trending',
                    'url': '/services/data/v67.0/wave/datasets/0Fb140000000000000',
                },
            ],
            'dateVersion': 1,
            'files': [],
            'filesUrl': '/services/data/v67.0/wave/lenses/0FK200000000000000/files',
            'folder': {
                'id': '00l040000000000AAA',
                'label': 'salestest',
                'name': 'salestest',
                'url': '/services/data/v67.0/wave/folders/00l040000000000AAA',
            },
            'historiesUrl': '/services/data/v67.0/wave/lenses/0FK200000000000000/histories',
            'id': '0FK200000000000000',
            'label': 'Historical Pipeline By Forecast',
            'lastAccessedDate': '2026-10-05T20:39:46.000Z',
            'lastModifiedBy': {
                'id': '005020000000000AAA',
                'name': 'Example User',
                'profilePhotoUrl': 'https://example-org.my.salesforce.com/profilephoto/005/T',
            },
            'lastModifiedDate': '2026-07-15T20:12:55.000Z',
            'name': 'Historical_Pipeline_By_Forecast',
            'permissions': {
                'create': True,
                'manage': True,
                'modify': True,
                'view': True,
            },
            'refreshDate': '2026-09-20T02:41:42.000Z',
            'templateAssetSourceName': 'Historical_Pipeline_By_Forecast',
            'templateSourceId': 'sfdc_internal__Sales_Analytics_Flex',
            'type': 'lens',
            'url': '/services/data/v67.0/wave/lenses/0FK200000000000000',
            'visibility': 'All',
            'visualizationType': 'stackvbar',
        },
    ],
    'nextPageUrl': None,
    'totalSize': 6,
    'url': '/services/data/v67.0/wave/lenses',
}

# GET /services/data/{version}/wave/datasets/{id}/versions/{versionId}/xmds/main
# Dataset extended metadata.
#
# Top-level `dimensions` / `measures` / `dates` are lists of field
# definitions; the payload also carries a `dataset` sub-object and
# `derivedDimensions` / `derivedMeasures`.
DATASET_XMD = {
    'createdBy': {
        'id': '005050000000000AAA',
        'name': 'Example Integration User',
        'profilePhotoUrl': 'https://example-org.my.salesforce.com/profilephoto/005/T',
    },
    'createdDate': '2026-09-20T02:42:06.000Z',
    'dataset': {
        'connector': 'API',
        'fullyQualifiedName': 'Account',
    },
    'dates': [
        {
            'alias': 'Last Modified Date',
            'compact': False,
            'description': 'LastModifiedDate',
            'fields': {
                'day': 'LastModifiedDate_Day',
                'epochDay': 'LastModifiedDate_day_epoch',
                'epochSecond': 'LastModifiedDate_sec_epoch',
                'fullField': 'LastModifiedDate',
                'hour': 'LastModifiedDate_Hour',
                'minute': 'LastModifiedDate_Minute',
                'month': 'LastModifiedDate_Month',
                'quarter': 'LastModifiedDate_Quarter',
                'second': 'LastModifiedDate_Second',
                'week': 'LastModifiedDate_Week',
                'year': 'LastModifiedDate_Year',
            },
            'firstDayOfWeek': 0,
            'fiscalMonthOffset': 0,
            'format': 'yyyy-MM-ddTHH:mm:ss.SSSZ',
            'fullyQualifiedName': 'Account.LastModifiedDate',
            'isYearEndFiscalYear': False,
            'label': 'Last Modified Date',
            'showInExplorer': True,
            'type': 'Date',
        },
        {
            'alias': 'Created Date',
            'compact': False,
            'description': 'CreatedDate',
            'fields': {
                'day': 'CreatedDate_Day',
                'epochDay': 'CreatedDate_day_epoch',
                'epochSecond': 'CreatedDate_sec_epoch',
                'fullField': 'CreatedDate',
                'hour': 'CreatedDate_Hour',
                'minute': 'CreatedDate_Minute',
                'month': 'CreatedDate_Month',
                'quarter': 'CreatedDate_Quarter',
                'second': 'CreatedDate_Second',
                'week': 'CreatedDate_Week',
                'year': 'CreatedDate_Year',
            },
            'firstDayOfWeek': 0,
            'fiscalMonthOffset': 0,
            'format': 'yyyy-MM-ddTHH:mm:ss.SSSZ',
            'fullyQualifiedName': 'Account.CreatedDate',
            'isYearEndFiscalYear': True,
            'label': 'Created Date',
            'showInExplorer': True,
            'type': 'Date',
        },
    ],
    'derivedDimensions': [],
    'derivedMeasures': [
        {
            'conditionalFormatting': {},
            'field': 'TotalPrice',
            'format': {
                'customFormat': '[&quot;$#,###&quot;,1]',
                'decimalDigits': 2,
                'delimiters': {},
            },
            'label': 'Total Price',
        },
    ],
    'dimensions': [
        {
            'conditionalFormatting': {},
            'customActions': [],
            'customActionsEnabled': True,
            'field': 'LastModifiedDate',
            'isMultiValue': False,
            'label': 'LastModifiedDate',
            'linkTemplateEnabled': True,
            'members': [],
            'recordDisplayFields': [],
            'salesforceActions': [],
            'salesforceActionsEnabled': True,
            'showInExplorer': True,
        },
        {
            'conditionalFormatting': {},
            'customActions': [],
            'customActionsEnabled': True,
            'field': 'LastModifiedDate_Year',
            'isMultiValue': False,
            'label': 'LastModifiedDate_Year',
            'linkTemplateEnabled': True,
            'members': [],
            'recordDisplayFields': [],
            'salesforceActions': [],
            'salesforceActionsEnabled': True,
            'showInExplorer': True,
        },
    ],
    'language': 'en_US',
    'lastModifiedBy': {
        'id': '005050000000000AAA',
        'name': 'Example Integration User',
        'profilePhotoUrl': 'https://example-org.my.salesforce.com/profilephoto/005/T',
    },
    'lastModifiedDate': '2026-09-20T02:42:06.000Z',
    'measures': [
        {
            'conditionalFormatting': {},
            'field': 'LastModifiedDate_day_epoch',
            'format': {
                'delimiters': {},
            },
            'label': 'LastModifiedDate_day_epoch',
            'showInExplorer': True,
        },
        {
            'conditionalFormatting': {},
            'field': 'LastModifiedDate_sec_epoch',
            'format': {
                'delimiters': {},
            },
            'label': 'LastModifiedDate_sec_epoch',
            'showInExplorer': True,
        },
    ],
    'organizations': [],
    'showDetailsDefaultFields': [],
    'type': 'main',
    'url': '/services/data/v67.0/wave/datasets/0Fb010000000000000/versions/0Fc030000000000000/xmds/main',
}

# GET /services/data/{version}/wave/dependencies/{id}
# Downstream dependencies of a dataset. The `dashboards` key holds a
# `dependencies` list of the dashboards that consume the dataset.
DATASET_DEPENDENCIES = {
    'dashboards': {
        'dependencies': [
            {
                'assetSharingUrl': 'https://example-org.my.salesforce.com/analytics/wave/dashboard?assetId=0FK080000000000000&orgId=00D090000000000AAA&loginHost=example.my.salesforce.com&urlType=sharing',
                'folder': {
                    'id': '00l040000000000AAA',
                    'label': 'salestest',
                    'url': '/services/data/v67.0/wave/folders/00l040000000000AAA',
                },
                'id': '0FK080000000000000',
                'isArchived': False,
                'lastAccessedDate': '2026-10-05T20:39:44.000Z',
                'lastModifiedBy': {
                    'id': '005020000000000AAA',
                    'name': 'Example User',
                    'profilePhotoUrl': 'https://example-org.my.salesforce.com/profilephoto/005/T',
                },
                'lastModifiedDate': '2026-07-15T20:12:55.000Z',
                'name': 'Sales Rep Whitespace',
                'type': 'dashboard',
                'url': '/services/data/v67.0/wave/dashboards/0FK080000000000000',
            },
            {
                'assetSharingUrl': 'https://example-org.my.salesforce.com/analytics/wave/dashboard?assetId=0FK250000000000000&orgId=00D090000000000AAA&loginHost=example.my.salesforce.com&urlType=sharing',
                'folder': {
                    'id': '00l040000000000AAA',
                    'label': 'salestest',
                    'url': '/services/data/v67.0/wave/folders/00l040000000000AAA',
                },
                'id': '0FK250000000000000',
                'isArchived': False,
                'lastAccessedDate': '2026-10-05T20:39:42.000Z',
                'lastModifiedBy': {
                    'id': '005020000000000AAA',
                    'name': 'Example User',
                    'profilePhotoUrl': 'https://example-org.my.salesforce.com/profilephoto/005/T',
                },
                'lastModifiedDate': '2026-07-15T20:12:55.000Z',
                'name': 'Sales Rep Overview',
                'type': 'dashboard',
                'url': '/services/data/v67.0/wave/dashboards/0FK250000000000000',
            },
        ],
        'totalSize': 4,
    },
    'folder': {
        'id': '00l040000000000AAA',
        'label': 'salestest',
        'url': '/services/data/v67.0/wave/folders/00l040000000000AAA',
    },
    'id': '0Fb010000000000000',
    'lastModifiedBy': {
        'id': '005050000000000AAA',
        'name': 'Example Integration User',
        'profilePhotoUrl': 'https://example-org.my.salesforce.com/profilephoto/005/T',
    },
    'lastModifiedDate': '2026-10-05T20:39:49.000Z',
    'name': 'Accounts',
    'type': 'dataset',
    'url': '/services/data/v67.0/wave/datasets/0Fb010000000000000',
}

# GET /services/data/{version}/wave/datasets/{id}/versions
# Dataset version listing.
#
# The owning dataset is a nested `dataset` object, not a flat `datasetId`.
# There is no `versionNumber` and no `status`.
DATASET_VERSIONS = {
    'url': '/services/data/v67.0/wave/datasets/0Fb010000000000000/versions',
    'versions': [
        {
            'createdBy': {
                'id': '005050000000000AAA',
                'name': 'Example Integration User',
                'profilePhotoUrl': 'https://example-org.my.salesforce.com/profilephoto/005/T',
            },
            'createdDate': '2026-09-20T02:42:03.000Z',
            'dataset': {
                'id': '0Fb010000000000000',
                'url': '/services/data/v67.0/wave/datasets/0Fb010000000000000',
            },
            'files': [],
            'id': '0Fc030000000000000',
            'lastModifiedBy': {
                'id': '005050000000000AAA',
                'name': 'Example Integration User',
                'profilePhotoUrl': 'https://example-org.my.salesforce.com/profilephoto/005/T',
            },
            'lastModifiedDate': '2026-10-21T13:58:05.000Z',
            'permissions': {
                'create': True,
                'modify': True,
                'view': True,
            },
            'predicate': '&#39;Owner.Role.Roles&#39; == &quot;$User.UserRoleId&quot; || &#39;OwnerId&#39; == &quot;$User.Id&quot;',
            'predicateVersion': 1.0,
            'securityCoverageUrl': '/services/data/v67.0/wave/security/coverage/datasets/0Fb010000000000000/versions/0Fc030000000000000',
            'totalRowCount': 1001,
            'type': 'datasetversion',
            'url': '/services/data/v67.0/wave/datasets/0Fb010000000000000/versions/0Fc030000000000000',
            'xmdsUrl': '/services/data/v67.0/wave/datasets/0Fb010000000000000/versions/0Fc030000000000000/xmds',
        },
        {
            'createdBy': {
                'id': '005050000000000AAA',
                'name': 'Example Integration User',
                'profilePhotoUrl': 'https://example-org.my.salesforce.com/profilephoto/005/T',
            },
            'createdDate': '2026-10-21T13:57:31.000Z',
            'dataset': {
                'id': '0Fb010000000000000',
                'url': '/services/data/v67.0/wave/datasets/0Fb010000000000000',
            },
            'files': [],
            'id': '0Fc330000000000000',
            'lastModifiedBy': {
                'id': '005050000000000AAA',
                'name': 'Example Integration User',
                'profilePhotoUrl': 'https://example-org.my.salesforce.com/profilephoto/005/T',
            },
            'lastModifiedDate': '2026-10-21T13:58:05.000Z',
            'permissions': {
                'create': True,
                'modify': True,
                'view': True,
            },
            'predicate': '&#39;Owner.Role.Roles&#39; == &quot;$User.UserRoleId&quot; || &#39;OwnerId&#39; == &quot;$User.Id&quot;',
            'predicateVersion': 1.0,
            'securityCoverageUrl': '/services/data/v67.0/wave/security/coverage/datasets/0Fb010000000000000/versions/0Fc330000000000000',
            'totalRowCount': 1001,
            'type': 'datasetversion',
            'url': '/services/data/v67.0/wave/datasets/0Fb010000000000000/versions/0Fc330000000000000',
            'xmdsUrl': '/services/data/v67.0/wave/datasets/0Fb010000000000000/versions/0Fc330000000000000/xmds',
        },
    ],
}

# GET /services/data/{version}/limits
# API limits.
#
# A flat mapping of limit name -> {Max, Remaining}, NOT a list.
LIMITS = {
    'AnalyticsExternalDataSizeMB': {
        'Max': 40960,
        'Remaining': 40960,
    },
    'CdpAiInferenceApiMonthlyLimit': {
        'Max': 500000000,
        'Remaining': 500000000,
    },
    'ConcurrentAsyncGetReportInstances': {
        'Max': 200,
        'Remaining': 200,
    },
    'ConcurrentEinsteinDataInsightsStoryCreation': {
        'Max': 5,
        'Remaining': 5,
    },
    'ConcurrentEinsteinDiscoveryStoryCreation': {
        'Max': 2,
        'Remaining': 2,
    },
    'ConcurrentSyncReportRuns': {
        'Max': 20,
        'Remaining': 20,
    },
}

# GET /services/data/{version}/wave/replicatedDatasets
# Replicated dataset listing. Each entry carries `datasetId`,
# `replicationDataflowId`, `sourceObjectName`, `connector` and `status`.
REPLICATED_DATASETS = {
    'replicatedDatasets': [
        {
            'advancedProperties': [],
            'connectionMode': 'incremental',
            'connector': {
                'connectionProperties': [],
                'connectorHandler': 'Legacy',
                'connectorType': 'SfdcLocal',
                'createdBy': {
                    'id': '005050000000000AAA',
                    'name': 'Example Integration User',
                    'profilePhotoUrl': '/img/userprofile/default_profile_45_v2.png',
                },
                'createdDate': '2026-07-15T20:12:55.000Z',
                'description': 'SFDC_LOCAL',
                'id': '0It280000000000AAA',
                'ingestionSchedule': {
                    'assetId': '0It280000000000AAA',
                    'daysOfWeek': [],
                    'events': {
                        'events': [],
                    },
                    'frequency': 'weekly',
                    'notificationLevel': 'never',
                },
                'label': 'SFDC_LOCAL',
                'lastModifiedBy': {
                    'id': '005020000000000AAA',
                    'name': 'Example User',
                    'profilePhotoUrl': '/img/userprofile/default_profile_45_v2.png',
                },
                'lastModifiedDate': '2026-07-15T20:12:55.000Z',
                'name': 'SFDC_LOCAL',
                'type': 'dataconnector',
                'url': '/services/data/v67.0/wave/dataConnectors/0It280000000000AAA',
            },
            'createdBy': {
                'id': '005020000000000AAA',
                'name': 'Example User',
                'profilePhotoUrl': '/img/userprofile/default_profile_45_v2.png',
            },
            'createdDate': '2026-07-15T20:12:55.000Z',
            'datasetId': '0Fb290000000000000',
            'fieldCount': 12,
            'fieldsUrl': '/services/data/v67.0/wave/replicatedDatasets/0Iuak000000JFWXCA4/fields',
            'filterApplied': False,
            'id': '0Iuak000000JFWXCA4',
            'label': 'PricebookEntry',
            'lastModifiedBy': {
                'id': '005020000000000AAA',
                'name': 'Example User',
                'profilePhotoUrl': '/img/userprofile/default_profile_45_v2.png',
            },
            'lastModifiedDate': '2026-07-15T20:12:55.000Z',
            'lastRefreshedDate': '2026-10-21T13:51:59.000Z',
            'name': 'PricebookEntry',
            'parquetOutputEnabled': False,
            'replicationDataflowId': '02K300000000000AAA',
            'sourceObjectName': 'PricebookEntry',
            'status': 'running',
            'supportedConnectionModes': [
                'full',
                'incremental',
            ],
            'type': 'replicateddataset',
            'url': '/services/data/v67.0/wave/replicatedDatasets/0Iuak000000JFWXCA4',
            'uuid': '0Iuak000000JFWXCA4',
        },
        {
            'advancedProperties': [],
            'connectionMode': 'incremental',
            'connector': {
                'connectionProperties': [],
                'connectorHandler': 'Legacy',
                'connectorType': 'SfdcLocal',
                'createdBy': {
                    'id': '005050000000000AAA',
                    'name': 'Example Integration User',
                    'profilePhotoUrl': '/img/userprofile/default_profile_45_v2.png',
                },
                'createdDate': '2026-07-15T20:12:55.000Z',
                'description': 'SFDC_LOCAL',
                'id': '0It280000000000AAA',
                'ingestionSchedule': {
                    'assetId': '0It280000000000AAA',
                    'daysOfWeek': [],
                    'events': {
                        'events': [],
                    },
                    'frequency': 'weekly',
                    'notificationLevel': 'never',
                },
                'label': 'SFDC_LOCAL',
                'lastModifiedBy': {
                    'id': '005020000000000AAA',
                    'name': 'Example User',
                    'profilePhotoUrl': '/img/userprofile/default_profile_45_v2.png',
                },
                'lastModifiedDate': '2026-07-15T20:12:55.000Z',
                'name': 'SFDC_LOCAL',
                'type': 'dataconnector',
                'url': '/services/data/v67.0/wave/dataConnectors/0It280000000000AAA',
            },
            'createdBy': {
                'id': '005020000000000AAA',
                'name': 'Example User',
                'profilePhotoUrl': '/img/userprofile/default_profile_45_v2.png',
            },
            'createdDate': '2026-07-15T20:12:55.000Z',
            'datasetId': '0Fb310000000000000',
            'fieldCount': 9,
            'fieldsUrl': '/services/data/v67.0/wave/replicatedDatasets/0Iuak000000JFWYCA4/fields',
            'filterApplied': False,
            'id': '0Iuak000000JFWYCA4',
            'label': 'OpportunityHistory',
            'lastModifiedBy': {
                'id': '005020000000000AAA',
                'name': 'Example User',
                'profilePhotoUrl': '/img/userprofile/default_profile_45_v2.png',
            },
            'lastModifiedDate': '2026-07-15T20:12:55.000Z',
            'lastRefreshedDate': '2026-10-21T13:51:59.000Z',
            'name': 'OpportunityHistory',
            'parquetOutputEnabled': False,
            'replicationDataflowId': '02K320000000000AAA',
            'sourceObjectName': 'OpportunityHistory',
            'status': 'running',
            'supportedConnectionModes': [
                'full',
                'incremental',
            ],
            'type': 'replicateddataset',
            'url': '/services/data/v67.0/wave/replicatedDatasets/0Iuak000000JFWYCA4',
            'uuid': '0Iuak000000JFWYCA4',
        },
    ],
    'url': '/services/data/v67.0/wave/replicatedDatasets',
}
