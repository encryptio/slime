package Utils;
use warnings;
use strict;

require Exporter;
our @ISA = qw/ Exporter /;
our @EXPORT = qw/ start_process wait_for status build_binary_dir wait_port_connectable setup_basic_slime_cluster kill_wait_for require_files_equal /;

use File::Temp ();
use Time::HiRes qw/ sleep time /;
use IO::Socket::INET ();

sub status {
    print STDERR ">>> ".join(" ", @_)."\n";
}

my @pids;
sub start_process {
    my ($cmd) = @_;
    my $pid = fork;
    die if not defined $pid;
    if ( !$pid ) {
        # child
        @pids = ();
        exec "sh", "-c", $cmd;
        die "Couldn't exec: $!";
    }
    push @pids, $pid;
    return $pid;
}

sub kill_wait_for {
    my ($pid) = @_;
    kill 15, $pid; # SIGTERM
    wait_for($pid);
}

sub wait_for {
    my ($pid) = @_;
    my $waitpid = waitpid $pid, 0;
    die if $waitpid < 0;
    @pids = grep { $_ != $pid } @pids;
    return $?;
}

END {
    while ( @pids ) {
        my $pid = $pids[0];
        print "Killing leaked process $pid\n";
        kill 9, $pid; # SIGKILL
        wait_for $pid;
    }
}

sub build_binary_dir {
    my ($config) = @_;
    $config = {} unless $config;
    $config = {(
        race => 0,
    ), %$config};

    status "building binaries";
    my $binaries = File::Temp->newdir;

    my $flags = "";
    $flags .= "-race " if $config->{race};

    system "go build $flags -o \Q$binaries/slime\E github.com/encryptio/slime" and die;
    system "go build $flags -o \Q$binaries/slimectl\E github.com/encryptio/slime/slimectl" and die;
    system "go build $flags -o \Q$binaries/git-annex-remote-slime\E github.com/encryptio/slime/misc/git-annex-remote-slime" and die;
    system "go build $flags -o \Q$binaries/git-annex-serve-from-slime\E github.com/encryptio/slime/misc/git-annex-serve-from-slime" and die;
    $ENV{PATH} = "$binaries:$ENV{PATH}";
    return $binaries;
}

sub wait_port_connectable {
    my ($addr, $max_wait) = @_;
    $max_wait = 10 unless $max_wait;

    my $start_time = time;
    while ( 1 ) {
        my $sock = IO::Socket::INET->new(
            PeerAddr => $addr,
            Timeout => 0.2,
        );
        if ( $sock ) {
            $sock->close;
            last;
        } else {
            sleep 0.1;
            die "Port $addr not connectable" if time() - $start_time > $max_wait;
        }
    }
}

sub setup_basic_slime_cluster {
    my ($config) = @_;
    $config = {} unless $config;

    status "Setting up basic slime cluster";

    my $tmp_dir = File::Temp->newdir;
    mkdir "$tmp_dir/chunk_data" or die;

    # default values
    $config = {(
        proxy_listen => "127.0.0.1:59000",
        chunk_listen => "127.0.0.1:59001",
        db_type => "bolt",
        db_dsn => "$tmp_dir/proxy-db.bolt",
        chunk_dirs => ["$tmp_dir/chunk_data"],
        log_http => 0,
    ), %$config};

    my $dirs_str = "";
    for my $dir ( @{$config->{chunk_dirs}} ) {
        $dirs_str .= "    \"$dir\",\n";
    }

    my $disable_http_logging = $config->{log_http} ? "false" : "true";

    open my $sf, ">", "$tmp_dir/server.toml" or die;
    print $sf <<EOF;
[proxy]
listen = "$config->{proxy_listen}"
disable-http-logging = $disable_http_logging

[proxy.database]
type = "$config->{db_type}"
dsn = "$config->{db_dsn}"

[chunk]
listen = "$config->{chunk_listen}"
disable-http-logging = $disable_http_logging

dirs = [
$dirs_str
]
EOF
    close $sf or die;

    $config->{proxy_pid} = start_process "slime proxy-server \Q$tmp_dir/server.toml\E";

    for my $dir ( @{$config->{chunk_dirs}} ) {
        system "slime fmt-dir \Q$dir\E" and die;
    }
    $config->{chunk_pid} = start_process "slime chunk-server \Q$tmp_dir/server.toml\E";
    wait_port_connectable $_ for @{$config}{"proxy_listen", "chunk_listen"};

    system "slimectl -base http://$config->{proxy_listen}/ store scan http://$config->{chunk_listen}" and die;

    system "slimectl -base http://$config->{proxy_listen}/ redundancy set 1 1" and die;

    return ($config, $tmp_dir);
}

sub require_files_equal {
    my ($fn1, $fn2) = @_;
    open my $lf1, "<", $fn1 or die;
    my $contents1 = do { local $/; <$lf1> };
    close $lf1;
    open my $lf2, "<", $fn2 or die;
    my $contents2 = do { local $/; <$lf2> };
    close $lf2;
    die "$fn1 is not equal to $fn2" if $contents1 ne $contents2;
}

1;
