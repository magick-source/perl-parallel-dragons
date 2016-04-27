package Parallel::Dragons;

use 5.022001;
use strict;
use warnings;

our $VERSION = '0.9.1';

use Carp;
use Socket;
use IO::Select;
use IO::Socket::UNIX;
use Time::HiRes qw( usleep );

use POSIX qw(WNOHANG);

use Data::Dumper;

#######################################################################
# DEBUG and ERRORs
#######################################################################

BEGIN {
    my $level = ($ENV{DRAGONS_DEBUG}||2) + 0;
    my $cron  = $ENV{DRAGONS_CRONJOB}||$ENV{IS_CRONJOB}||0;
    my $debug = $level>2 ? 1 : 0;
    my $info  = $level ;
    my $trace = $level>9 ? 1 : 0;
    eval <<EoC;
sub _debug { $debug }
sub _info { $info }
sub _trace { $trace }
sub IS_CRONJOB { $cron }
EoC
}


sub DEBUG {
    return unless _debug;
    return unless @_;
    my $mask = shift;
    local $Data::Dumper::Indent = 0;
    map { ref $_ and $_ = Dumper($_) } @_;
    my @args = $mask =~ m{\%} ? (sprintf $mask, @_) : ($mask, @_);
    print STDERR "$$: ", time, " DEBUG: ", @args,"\n";
}

sub INFO {
    return unless _info;
    return unless @_;
    my $mask = shift;
    local $Data::Dumper::Indent = 0;
    map { ref $_ and $_ = Dumper($_) } @_;
    my @args = $mask =~ m{\%} ? (sprintf $mask, @_) : ($mask, @_);
    print STDERR "$$: ", time, " INFO: ", @args,"\n";
}

sub TRACE {
    return unless _trace;
    return unless @_;
    my $mask = shift;
    local $Data::Dumper::Indent = 0;
    map { ref $_ and $_ = Dumper($_) } @_;
    my @args = $mask =~ m{\%} ? (sprintf $mask, @_) : ($mask, @_);
    print STDERR "$$: ", time, " TRACE: ", @args,"\n";
}

sub FATAL {
    croak @_;
}

#######################################################################
# Main code
#######################################################################


sub new { 
    my $class = shift;

    my $self = bless {}, $class;

    $self->init();

    return $self;
}

sub init {
    my $self = shift;

    $self->{max_childs} = $self->can('max_childs')
        ? $self->max_childs
        : $self->server_cores;

    $self->{need_childs} = $self->{max_childs};

    $self->{tmp_basename} = $self->sockfile(); 

    $self->{_wait} = $self->can('wait_before_restart')
        ? $self->wait_before_restart
        : 3;
    $self->{_restart_next} = 0;

    $self->{_dynamic_wait} = $self->can('restart_next')
        ? 1 
        : 0;
    
    $self->{max_tasks} = $self->can('restart_after')
        ? $self->restart_after
        : 0;

    $self->{max_memory} = $self->can('max_memory_per_child')
        ? $self->max_memory_per_child
        : 0;

    if ($self->can('post_child_exit')) {
        $self->{__post_exit} = sub {
                $self->post_child_exit(@_);
            };  
    }   

    $self->{_destroyed} = 0;
    $self->{_childs} = []; 
    $self->{resident} = 0;

    $self->{_monitor_called} = 0;

    return;
}

sub _destroy {
    my ($self) = @_; 

    return if $self->{_destroyed};

    if ( $self->resident() ) { 
        INFO "Stopping running child processed";

        my @pids_to_stop = map { $_->{pid} } @{$self->{_childs}};
        TRACE "Killing pids: @pids_to_stop";
        kill 'INT', @pids_to_stop;

        INFO "Removing communication socket %s", $self->sockfile;
        my $sfile = $self->sockfile;
        $sfile .= '.sock';
        unlink $sfile;
    }

    $self->{_destroyed} = 1;
}

sub DESTROY {
    my $self = shift;

    $self->_destroy;
}

sub set_resident {
    $_[0]->{resident} = $_[1];
}

sub resident {
    $_[0]->{resident};
}

sub _get_pid {
    my $self = shift;

    my $pidfile = $self->{tmp_basename}.'.pid';
    if (-e $pidfile) {
        open my $fh, '<', $pidfile;
        my ($pid) = <$fh>;

        $pid+=0;

        return $pid;
    }

    return 0;
}

sub check_pid {
    my $self = shift;

    my $pid = $self->_get_pid;

    if ($pid and $pid == $$) {
        return 1;
    }

    return 0;
}

my %commands = (
    start       => '_cmd_start_daemon',
    startfg     => '_cmd_start_foreground',
    stop        => '_cmd_stop_daemon',
    help        => '_cmd_help_daemon',

    __default__ => '_cmd_forward_to_daemon',
);
$commands{foreground}  = $commands{startfg};

sub run {
    my $self = shift;

    TRACE "starting run with %s", join(" ", @ARGV);

    my @args = @ARGV ? @ARGV : ('help');

    while (@args) {
        my $cmd = shift @args;
        $cmd = lc($cmd);
        my $method;
        if ( $commands{$cmd} ) {
            TRACE "Running command '$cmd'";
            $method = $commands{$cmd};
        } else {
            TRACE "Running command '$cmd' with __default__";
            $method = $commands{ __default__ };
        }
        $self->$method( $cmd, \@args) if $method
    }
}

sub server_cores {
    open my $fh, '<', '/proc/stat';
    my $cores = 0;
    while (my $ln = <$fh>) {
        if ($ln=~m{^cpu(\d+)}) {
            $cores = 1+ $1;
        }
    }

    return $cores;
}

sub vmsize {
    local $/ = undef;
    open my $fh, '<', "/proc/$$/status";

    my $size = (<$fh> =~ m{^VmSize:\s*(\d+)}m)[0];
    return $size * 1024;
}

sub childs_running {
  my $self = shift;

  return scalar @{ $self->{_childs} || [] };
}

sub sockfile {
    my $self = shift;

    my ($fname) = $ENV{DRAGON_SOCKET_FILE};
    if ($fname) {
        $fname =~ s{\W+}{-}g;
    } else {
        ($fname) = $0 =~ m{([^/]+)(?:.pl)$};
        $fname = 'there-are-dragons' unless $fname;
    }

    return "/tmp/$fname";
}

sub get_client_socket {
    my $self = shift;

    return $self->{client_socket}
        if $self->{client_socket};

    my $sockfile = $self->sockfile;
    $sockfile .= '.sock';
    return unless -e $sockfile;

    return $self->{client_socket}
        = IO::Socket::UNIX->new(
                Peer    => $sockfile,
                Timeout => 3,
            );
}

sub get_listen_socket {
    my $self = shift;
    return $self->{server_socket}
        if $self->{server_socket};

    my $sfile = $self->sockfile;
    $sfile .= '.sock';

    unlink $sfile if -e $sfile;

    $self->{server_socket} = IO::Socket::UNIX->new(
            Local   => $sfile,
            Type    => SOCK_STREAM,
            Listen  => 5
        );

    chmod 0777, $sfile;

    my $pfile = $self->sockfile;
    $pfile .= '.pid';
    open my $fh, '>', $pfile;
    print $fh $$;
    close $pfile;

    return $self->{server_socket};
}

sub daemon_name {
  my $self = shift;

  my $name = $self->sockfile;
  ($name) = (split m{/}, $name)[-1];
  return "Dragons of $name";
}

sub check_daemon {
    my $self = shift;

    if (my $pid = $self->_get_pid()) {
        my $cnt = kill 0, $pid;
        return $cnt;

    } else {
        my $sock = $self->get_client_socket();

        if ($sock) {
            print $sock "ping\n";
            while ( my $res = <$sock> ) {
                if ($res =~ m{^pong\s*$}i) {
                    return 1;
                }
            }
        }
    }

    return 0;
}

sub stop {
    my $self = shift;
    $self->{stopping} = 1;

    my @pids_to_stop = map { $_->{pid} } @{$self->{_childs}};
    TRACE "Notifying the family: @pids_to_stop";
    kill 'USR1', @pids_to_stop if @pids_to_stop;
}

sub fork_child {
	my ($self, $sub) = @_;

    my $pid= fork;
    if ($pid) {
        # we are in the parent, and we got a kid
        my %pid = (
            pid         => $pid,
            start_time  => time,
        );

        push @{ $self->{_childs} }, \%pid;
    
        return \%pid;
    } elsif (defined $pid) {
        srand();
        eval {
            $sub->();
            1;
        } or do {
            my $error = $@ || 'Zombie error';
            FATAL "Error in child process: %s", $error;
        };

        exit 0;
    }

    FATAL "Could not fork child process: $!";
}

sub wait_for_children {
    my $self = shift;

    my @done;

    @{ $self->{_childs} } = grep {
        my $pid= waitpid $_->{pid}, WNOHANG;

        if ($pid) {
            my $ec= $?;
            if ($ec & 127) {
                $_->{exit_code}= $ec;
                $_->{exit_with_signal}= ($ec & 127);
                $_->{exit_with_coredump}= !!($ec & 128);
            } else {
                $_->{exit_code}= $ec >> 8;
            }

            $_->{exit_time}= time;
            $_->{elapsed_time}= $_->{exit_time} - $_->{start_time};

            push @done, $_;
        }

        !$pid;
    } @{ $self->{_childs} };

    return \@done;
}

sub is_foreground {
  my ($self) = @_;

  $self->{_foreground};
}

################################################################
## Child code - init and run the child process/task
################################################################

sub halt {
    my $self = shift;

    $self->{_halt} = 1;
}

sub child_main_run {
    my $self = shift;

    $SIG{USR1} = sub {
        $self->halt;
    };

    TRACE "Starting child_main_run";

    $self->init_child
        if $self->can('init_child');

    if ($self->can('data_consumer')) {
        TRACE "Going to get a data consumer";
        my $dc = $self->data_consumer();
        $dc->consume( sub { $self->_dc_wrapper( @_ ) } );

    } elsif ( $self->can('claim_task') ) {
        TRACE "Using claim task";
        $self->_claim_wrapper();

    } else {
        $self->_nodc_wrapper();
    }

    $self->end_child
        if $self->can('end_child');
}

sub _dc_wrapper {
    my ($self, $dc, $id) = @_;

    my $error = '';
    eval {
        $self->main($id);
        1;
    } or do {
        $error = $@ || 'zombie error';
    };

    $self->_task_done($error);
    $dc->halt if $self->{_halt};
}

sub _claim_wrapper {
    my $self = shift;

    TASK:
    while (!$self->{_halt}) {
        my $error;

        my $task;
        eval {
            $task = $self->claim_task(); 1
        } or do {
            $error = $@ || 'zombie error';
            FATAL "error claiming task: $error";
        };

        last unless $task;
        eval {
            $self->main($task);
            $self->task_done($task)
                if $self->can('task_done');
            1;
        } or do {
            $error = $@ || 'zombie error';
            $self->task_failed($task)
                if $self->can('task_failed');
        };

        $self->_task_done($error);
    }
}

sub _nodc_wrapper {
    my $self = shift;

    # let's assume that if we don't have a Data::Consumer,
    # main know what it needs to do.
    eval {
        $self->main();
        1;
    } or do {
        my $error = $@ || 'zombie error';
        warn $error,"\n";
    };
}

sub _task_done {
    my ($self, $error) = @_;

    $self->{tasks_done}++;
    $self->{vmsize} = vmsize;

    TRACE "TASKDONE: $self->{tasks_done}: $self->{vmsize}";
    INFO " ==> %s \n===END OF ERROR", $error if $error;

    if ($self->{max_tasks} and $self->{tasks_done} >= $self->{max_tasks}) {
        INFO "STOPPING CHILD: done $self->{tasks_done} tasks";
        $self->halt;
    }
    if ($self->{max_memory} and $self->{vmsize} >= $self->{max_memory}) {
        INFO "STOPPING CHILD: exceeded max memory '$self->{vmsize}' of '$self->{max_memory}' used";
        $self->halt;
    }

    return;
}


#################################################################
## Daemon control methods - command line handlers
#################################################################

sub _cmd_start_daemon {
    my $self = shift;

    if ($self->check_daemon()) {
        INFO "Daemon is running - not starting again" unless IS_CRONJOB;
    } else {
        if ($self->can('pre_start')) {
            $self->pre_start();
            if ($self->{stopping}) {
                INFO "Start prevented by pre_start" unless IS_CRONJOB;
                return;
            }
        }
        my $pid = fork();
        if ($pid) {
            sleep 1;
            my $cnt= 30;
            $cnt = 60 if IS_CRONJOB;
            while ($cnt--) {
                if ($self->check_daemon()) {
                    INFO "Daemon Started" unless IS_CRONJOB;
                    return; #done starting
                }
                sleep 1;
            }
            FATAL "Not able to start the daemon";
        } elsif (defined $pid) {

            my $bfname = $self->sockfile;
            open (STDIN, '<','/dev/null');
            open STDOUT, '>>', $bfname.'.out'
                or die "Failed to redirect STDOUT";
            open STDERR, '>>', $bfname.'.err'
                or die "Failled to redirect STDERR";

            $self->_server_main();
        } else {
            FATAL "Not able to start the daemon";
        }
    }
}

sub _cmd_start_foreground {
    my $self = shift;

    TRACE "Starting in the foreground";

    $self->{_foreground} = 1;

    if ($self->can('pre_start')) {
        $self->pre_start();
        if ($self->{stopping}) {
            INFO "Start prevented by pre_start" unless IS_CRONJOB;
            return;
        }
    }
    if ($self->can('pre_fork')) {
        TRACE "Calling pre_fork";
        eval {
            $self->pre_fork();
            1;
        } or do {
            my $error = $@;
            FATAL "error on pre_fork: $error\n";
        }
    }

    $self->child_main_run();
}

sub _cmd_stop_daemon {
    my $self = shift;

    my $sock = $self->get_client_socket();

    # If there is no socket it is not running, so just exit.
    do {
        DEBUG "Daemon is not running";
        exit;
    } unless $sock;

    # If there is socket, forward the command to the daemon
    $self->_cmd_forward_to_daemon(@_);
}

sub _cmd_forward_to_daemon {
    my $self = shift;
    my ($cmd, $others) = @_;

    my $sock = $self->get_client_socket();

    FATAL "Can't connect to the Daemon"
        unless $sock;

    print $sock join ' ', $cmd, @$others, "\n";
    while (my $res = <$sock>) {
        print STDERR $res if -t STDERR;
        #reply are already \n terminated
    }

    @$others = ();
    return;
}

sub _cmd_help_daemon {
    print STDERR <<EoH;
Usage: $0 (help|startfg|start|stop|ping)

* help      - prints this screen
* startfg   - starts the dameon, but single processed, in the foreground
* start     - starts and daemonizes
* stop      - stops a running daemon
* ping      - checks if there is a running daemon (prints pong is true)


EoH

    return;
}

#######################################################################
# Daemon main
#######################################################################

sub _server_main {
    my $self = shift;

    my $sock = $self->get_listen_socket;

    my $select = IO::Select->new();
    $select->add( $sock );

    $self->set_resident(1);

    INFO "Daemon Started";

    $self->{_restart_next} = time + 3;
    # let the parent know that we are alive!

    while ( 1 ) {
        if (!$self->check_pid and !$self->{stopping}) {
            $self->stop;
            INFO "Stopping - other pid running\n";
        }

        $self->communicate()
            if $select->can_read( 0.01 );

        $self->_sm_start_child();

        usleep 100_000;
    }
}

sub _sm_start_child {
    my $self = shift;

    my $done = 0;

    my $finished = wait_for_children( $self );
    if ($finished and @$finished and $self->can('post_child_exit')) {
        $self->post_child_exit( $_ )
            for @$finished;

        $done += @$finished;
    }

    if ( $self->{_monitor_called} < time ) {
        $self->{_monitor_called} = time;
        $self->monitor
            if $self->can('monitor');

        $done++;
    }

    if ($self->{stopping}) {
        unless ( scalar @{$self->{_childs}} ) {
            INFO "Stopping Daemon";
            $self->pre_exit
                if $self->can('pre_exit');

            $self->_destroy;
            exit;
        }

        return;
    }

    return $self->_idle( $done )
      if $self->{_restart_next} > time;

    return $self->_idle( $done )
      if scalar @{$self->{_childs}} >= $self->{max_childs};

    $self->{need_childs} = $self->can('need_childs')
        ? $self->need_childs
        : $self->{max_childs};

    return $self->_idle( $done )
      if scalar @{$self->{_childs}} >= $self->{need_childs};

    TRACE "Going to start a child %d => %d",
        scalar @{$self->{_childs}}, $self->{need_childs};

    TRACE "can pre_fork: %s => %s", $self->can('pre_fork'),
        $self->can('pre_fork')?1:0;

    if ($self->can('pre_fork')) {
        TRACE "Calling pre_fork";
        eval {
            $self->pre_fork();
            1;
        } or do {
            my $error = $@;
            FATAL "error on pre_fork: $error\n";
        }
    }

    my $child = fork_child( # fork_child never returns on the child
        $self,
        sub {
            $self->set_resident(0);

            TRACE "Starting child_main_run";

            $self->child_main_run();
        }
    );

    if ($self->can('post_child_start') ) {
        TRACE "Calling post_child_start";
        eval {
            $self->post_child_start( $child );
            1;
        } or do {
            my $error = $@;
            FATAL "error on post_child_start: $error\n";
        }
    }

    if ($self->{_dynamic_wait}) {
        $self->{_restart_next} = $self->restart_next;
    } else {
        $self->{_restart_next} = time + $self->{_wait};
    }
}

sub _idle {
  my ($self,$done) = @_;

  return if $done;

  $self->idle()
    if $self->can('idle');

  return;
}

#######################################################################
# UnixSocket Client Session
#######################################################################

my %daemon_commands = (
    stop    => \&_us_cmd_stop_daemon,
    pause   => \&_us_cmd_pause_daemon,
    play    => \&_us_cmd_play_daemon,
    ping    => \&_us_cmd_ping,
    info    => \&_us_cmd_info,
);

sub communicate {
    my $self = shift;

    my $socket  = $self->get_listen_socket();
    my $conn    = $socket->accept();

    local $SIG{ALRM} = sub { TRACE "Timeout in communication attempt. Killing server process\n"; exit 1; };

    alarm(10);

    my $input = <$conn>;

    my @args = split m{\s+}, $input;

    while (my $cmd = shift @args) {
        $cmd = lc $cmd;
        if ($daemon_commands{ $cmd } ) {
            my @res = $daemon_commands{ $cmd }->(
                    $self, $cmd, \@args
                );

            TRACE "Replying to $cmd: ", @res;

            print $conn "$cmd reply:\n";
            print $conn $_,"\n" for @res;
        } else {
            print $conn "Command '$cmd' is not known\n";
        }

        if ($self->{stopping}) {
            TRACE "Stopping now";
            exit 0;
        }
    }

    alarm 0;
    $conn->close;
}

######################################################################
# UnixSocket command handlers - handle piped commands
######################################################################

sub _us_cmd_stop_daemon {
    my $self    = shift;
    my $cmd     = shift;
    my $args    = shift;

    INFO "Got a request to stop - waiting for childs and exiting";
    $self->stop;

    return ('stopping');
}

sub _us_cmd_ping {
    my $self = shift;
    my ($cmd, $args) = @_;

    return ('pong');
}

sub _us_cmd_info {
    my $self = shift;

    my $name    = $self->daemon_name;
    my $childs  = scalar @{ $self->{_childs} };

    my $next = localtime($self->{_restart_next});
    my $stopping = $self->{stopping} ? "\nDaemon is Stopping\n":"";

    my $res = <<EoI;
$name [server pid: $$]

Children: $childs of $self->{max_childs} running
    Next Start: $self->{_restart_next} [$next]

MaxTasks:   $self->{max_tasks}
MaxMemory:  $self->{max_memory}
$stopping


EoI

    return ($res);
}


1;
__END__

=head1 NAME

Parallel::Dragons - Daemon are forever... Dragons lay eggs, grow fast
and die in flames!

=head1 SYNOPSIS

  use parent qw'Parallel::Dragons';

  sub max_childs { 5 } # max 5 workers at a time

  sub wait_before_restart { 10 }

  sub main {
    my $i = 0;
    while ($i++ < 100) {
        print "$$: counted to $i\n";
        sleep 1;
    }

    print "counted to 100, going away\n";
  }

  main::->new()->run();

=head1 DESCRIPTION

Parallel::Dragons is a framework to create long lived processes that run
the same code all the time, in some cases multiple times in parallel.

It doesn't know, and doesn't care about what the code does. Even when
used in queue driven mode, it doesn't care about the queue, leaving the
implementation of the queue access to the application itself.


=head1 SEE ALSO

There are a lot of other perl packages to manage parallel execution
of code - depending on your use case some of them may be better than
Parallel::Dragons. This is just another option.

=head1 BUGS REPORTS and FEATURE REQUESTS

Please report any bugs , or request features in:

* github: https://github.com/themage/perl-parallel-dragons/
* Magick Source: http://magick-source.net/project/view/10/


=head1 AUTHOR

theMage E<lt>themage@magick-source.netE<gt>

=head1 COPYRIGHT AND LICENSE

Copyright (C) 2016 by theMage

This library is free software; you can redistribute it and/or modify
it under the same terms as Perl itself, either Perl version 5.22.1 or,
at your option, any later version of Perl 5 you may have available.

Alternativally, you can also redistribute it and/or modify it
under the terms of the GPL 2.0 licence (or any future version of it).

=cut

