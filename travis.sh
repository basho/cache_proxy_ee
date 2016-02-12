#!/bin/sh

# exit on error
set -e

# build cache-proxy
CFLAGS="-ggdb3 -O0" autoreconf -fvi && ./configure --enable-debug=log && make

# download devrel from s3
if [ -z $AWS_ACCESS_KEY ] || [ -z $AWS_SECRET_KEY ]; then
    echo "AWS_ACCESS_KEY and AWS_SECRET_KEY must be set"
    exit 1
fi
cat > ~/.s3cfg <<EOF
[default]
access_key = $AWS_ACCESS_KEY
secret_key = $AWS_SECRET_KEY
gpg_passphrase = $GPG_PASSPHRASE
access_token =
add_encoding_exts =
add_headers =
bucket_location = US
ca_certs_file =
cache_file =
check_ssl_certificate = True
check_ssl_hostname = True
cloudfront_host = cloudfront.amazonaws.com
default_mime_type = binary/octet-stream
delay_updates = False
delete_after = False
delete_after_fetch = False
delete_removed = False
dry_run = False
enable_multipart = True
encoding = UTF-8
encrypt = False
expiry_date =
expiry_days =
expiry_prefix =
follow_symlinks = False
force = False
get_continue = False
gpg_command = /usr/bin/gpg
gpg_decrypt = %(gpg_command)s -d --verbose --no-use-agent --batch --yes --passphrase-fd %(passphrase_fd)s -o %(output_file)s %(input_file)s
gpg_encrypt = %(gpg_command)s -c --verbose --no-use-agent --batch --yes --passphrase-fd %(passphrase_fd)s -o %(output_file)s %(input_file)s
guess_mime_type = True
host_base = s3.amazonaws.com
host_bucket = %(bucket)s.s3.amazonaws.com
human_readable_sizes = False
invalidate_default_index_on_cf = False
invalidate_default_index_root_on_cf = True
invalidate_on_cf = False
kms_key =
limitrate = 0
list_md5 = False
log_target_prefix =
long_listing = False
max_delete = -1
mime_type =
multipart_chunk_size_mb = 15
multipart_max_chunks = 10000
preserve_attrs = True
progress_meter = True
proxy_host =
proxy_port = 0
put_continue = False
recursive = False
recv_chunk = 4096
reduced_redundancy = False
requester_pays = False
restore_days = 1
send_chunk = 4096
server_side_encryption = False
signature_v2 = False
simpledb_host = sdb.amazonaws.com
skip_existing = False
socket_timeout = 300
stats = False
stop_on_error = False
storage_class =
urlencoding_mode = normal
use_https = True
use_mime_magic = True
verbosity = WARNING
website_endpoint = http://%(bucket)s.s3-website-%(location)s.amazonaws.com/
website_error =
website_index = index.html
EOF
echo "Downloading devrel from s3..."
s3cmd get $AWS_DEVREL_URL --no-progress > /dev/null
echo "Download devrel completed"
tar zxf ${AWS_DEVREL_URL##*/} dev/dev1

# copy binaries for tests
cp src/nutcracker tests/_binaries/
cp $(which redis-server) tests/_binaries/
cp $(which redis-cli) tests/_binaries/
(cd tests/ && RT_DEVREL_SRC=../dev/dev1 ./_binaries/create_riak_devrel_tarball.sh)

# perform tests
(cd tests/ && ./nosetests.sh)

