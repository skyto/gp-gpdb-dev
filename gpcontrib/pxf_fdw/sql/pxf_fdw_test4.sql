CREATE extension pxf_fdw;

CREATE SERVER demoserver
FOREIGN DATA WRAPPER pxf_fdw
OPTIONS (protocol 'demo');

CREATE USER MAPPING FOR gpadmin
SERVER demoserver
OPTIONS ("fs.s3a.awsAccessKeyId" 'your access key', "fs.s3a.awsSecretAccessKey" 'your secret key');

CREATE FOREIGN TABLE demo_table (first TEXT, last TEXT)
SERVER demoserver
OPTIONS (location 'pxf://tmp/tmp? \
		FRAGMENTER=org.greenplum.pxf.api.examples.DemoFragmenter& \
		ACCESSOR=org.greenplum.pxf.api.examples.DemoAccessor& \
		RESOLVER=org.greenplum.pxf.api.examples.DemoTextResolver');
