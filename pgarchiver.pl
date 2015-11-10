#!/usr/bin/env perl
#
# parallel archiving (rsyncing) of WAL files to multiple destinations
#
# usage: pgarchiver.pl src dst [dst...]
#
# (c) 2015 Christoph Moench-Tegeder <cmt@burggraben.net>
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.
#

use strict;
use warnings;

use IPC::Cmd qw(can_run);


sub main {
  my $dst = [];
  my ($src, $r);

  $src = shift(@ARGV);
  $dst = [ @ARGV ];
  $r = do_syncs($src, $dst);

  return $r;
}


sub do_syncs {
  my ($src, $dsts) = (shift, shift);
  my $jobinfo = {};
  my $args = {};
  my $ret = 0;
  my ($debug, $job, $js, $res, $runner);

  $args = { map { $_ => can_run($_) } (qw(rsync ssh)) };
  $jobinfo = [
    map {
      Internal::Rsync->new({%{$args}, 'src' => $src, 'dst' => $_})
    } (@{$dsts})
  ];

  $debug = exists($ENV{'ARCHIVEDEBUG'}) ? 1 : 0;
  $runner = Internal::Processor->new($jobinfo);
  $ret = $runner->execute();

  foreach $job (@{$jobinfo}) {
    $js = $job->status();
    if(!defined($js) || $js > 0) {
      print(STDERR 'ERROR: failure in ' . $job->tag() . "\n");
    } elsif($debug) {
      print(STDERR 'DEBUG: ' . $job->tag() . "\n");
    }
    if($js > 0) {
      $ret = ($ret == 0) ? $job->status() : $ret;
    }
    if(!defined($js) || $js > 0 || $debug) {
      print(STDERR $job->output() . "\n");
    }
    if($debug) {
      print(STDERR ' ' . $job->timer() . ' seconds' . "\n");
    }
    if(!defined($js) || $js > 0 || $debug) {
      print(STDERR "\n");
    }
  }

  return $ret;
}


exit(main());


package Internal::Cmd;

use strict;
use warnings;

use POSIX qw(:sys_wait_h);
use Storable qw(dclone);
use Time::HiRes qw(gettimeofday tv_interval);


sub new {
  my ($class, $args) = (shift, shift);
  my $defaults = {
    '_tag' => '',
    '_cmd' => [],
    '_env' => {}
  };
  my $self;

  $self = { %{$defaults}, %{$args || {}} };
  if(@{$self->{'_cmd'}} == 0) {
    return undef;
  }
  if($self->{'_tag'} eq '') {
    $self->{'_tag'} = join(' ', @{$self->{'_cmd'}});
  }
  $self->{'_started'} = undef;
  $self->{'_runtime'} = 0;
  $self->{'_pid'} = undef;
  $self->{'_data_out'} = '';
  $self->{'_data_err'} = '';

  $self = bless($self, $class);

  return $self;
}


sub cmdline {
  my $self = shift;

  return dclone($self->{'_cmd'});
}


sub env {
  my $self = shift;

  return dclone($self->{'_env'});
}


sub tag {
  my $self = shift;

  return $self->{'_tag'};
}


sub started {
  my $self = shift;

  if(!$self->{'_started'}) {
    $self->{'_started'} = [ gettimeofday() ];
  }

  return;
}


sub stopped {
  my ($self, $stat) = (shift, shift);

  if(!$self->{'_runtime'} && $self->{'_started'}) {
    $self->{'_runtime'} = tv_interval($self->{'_started'});
    $self->{'_exit'} = WIFEXITED($?) ? WEXITSTATUS($?) : undef;
    $self->{'_signal'} = WIFSIGNALED($?) ? WTERMSIG($?) : undef;
  }

  return;
}


sub failed {
  my ($self, $status) = (shift, shift);

  $self->{'_started'} = 0;
  $self->{'_runtime'} = 0;
  $self->{'_exit'} = -1;
  $self->{'_signal'} = 0;
  $self->{'_data_err'} = $status;

  return;
}


sub status {
  my $self = shift;

  return $self->{'_exit'};
}


sub output {
  my $self = shift;

  return $self->{'_data_out'} . "\n" . $self->{'_data_err'};
}


sub timer {
  my $self = shift;

  return $self->{'_runtime'};
}


sub record {
  my ($self, $kind, $data) = (shift, shift, shift);
  my $key;

  if($kind eq 'o') {
    $key = '_data_out';
  } elsif($kind eq 'e') {
    $key = '_data_err';
  } else {
    return -1;
  }
  $self->{$key}.= $data;

  return 0;
}

1;


package Internal::Rsync;

use strict;
use warnings;

use IPC::Cmd qw(can_run);

use base qw(Internal::Cmd);


sub new {
  my ($class, $args) = (shift, shift);
  my $defaults = {
    'persist' => 60,
    'timeout' => 5
  };
  my $obj = {};
  my $sshopts = [];
  my ($cmd, $data, $self, $tag);

  if(!$args || ref($args) ne 'HASH') {
    return undef;
  }
  foreach $tag (qw(src dst)) {
    if(!$args->{$tag} || ref($args->{$tag}) ne '') {
      return undef;
    }
  }
  $data = { %{$defaults}, %{$args} };
  $obj->{'_tag'} = exists($data->{'tag'}) ? $data->{'tag'} : $data->{'dst'};

  foreach $cmd (qw(rsync ssh)) {
    if($data->{$cmd}) {
      next;
    }
    $data->{$cmd} = can_run($cmd);
    if(!$data->{$cmd}) {
      print(STDERR 'cannot find ' . $cmd . "\n");
      return undef;
    }
  }

  $self = { %{$defaults}, %{$args} };
  $sshopts = [
    '-o ControlMaster=auto',
    '-o ControlPath=~/.ssh/%r@%h:%p.sock',
    '-o ControlPersist=' . $data->{'persist'},
    '-o ForwardX11=no',
    '-o ForwardAgent=no',
    '-o ConnectTimeout=' . $data->{'timeout'}
  ];
  $obj->{'_env'} = {
    'RSYNC_RSH' => join(' ', ($data->{'ssh'}, @{$sshopts}))
  };
  $obj->{'_cmd'} = [
    $data->{'rsync'},
    exists($ENV{'ARCHIVEDEBUG'}) ? '-av' : '-a',
    $data->{'src'}, $data->{'dst'}
  ];

  $self = bless($class->SUPER::new($obj), $class);

  return $self;
}

1;


package Internal::Processor;

use strict;
use warnings;

use Fcntl;
use POSIX;
use POSIX qw(:sys_wait_h);
use Symbol;


sub new {
  my ($class, $jobs, $args) = (shift, shift, shift);
  my $self = { %{$args || {}} };

  if(!defined($jobs) || ref($jobs) ne 'ARRAY') {
    return undef;
  }
  $self = bless($self, $class);
  $self->{'_jobs'} = {
    map { $_->tag() => $_ } (@{$jobs})
  };
  $self->{'_status'} = {
    map {
      $_ => {
        'or' => gensym(),
        'ow' => gensym(),
        'er' => gensym(),
        'ew' => gensym(),
        'pid' => undef
      }
    } (keys(%{$self->{'_jobs'}}))
  };

  $self->{'__active'} = {};
  $self->{'__exits'} = {};
  $self->{'_nfds'} = 0;
  $self->{'_fds'} = '';

  return $self;
}


sub execute {
  my ($self, $cmds) = (shift, shift);
  my $sptr = $self->{'_status'};
  my $pids = [];
  my $ret = 0;
  my ($cmd, $tag, $r);
  my ($act, $oldact, $oldset, $set, $chldset);

  $oldset = POSIX::SigSet->new();
  $chldset = POSIX::SigSet->new(SIGCHLD);
  $set = POSIX::SigSet->new(SIGTERM, SIGINT);
  $act = POSIX::SigAction->new(sub { $self->reaper() }, $set);
  $act->safe(1);
  $oldact = POSIX::SigAction->new();
  sigaction(SIGCHLD, $act, $oldact);

  foreach $tag (keys(%{$self->{'_jobs'}})) {
    $cmd = $self->{'_jobs'}->{$tag};
    $r = $self->run_cmd($cmd);
    $ret = ($r < 0) ? -1 : $ret;
  }

  # do not interrupt IO with SIGCHLD
  while($self->{'_nfds'} > 0) {
    sigprocmask(SIG_BLOCK, $chldset, $oldset);
    $self->collect_output();
    sigprocmask(SIG_SETMASK, $oldset);
  }

  do {
    $pids = [
      map {
        defined($sptr->{$_}->{'pid'}) ? $sptr->{$_}->{'pid'} : ()
      } (keys(%{$sptr}))
    ];
    $r = kill(0, @{$pids});
    if($r > 0) {
      sleep(0.05);
    }
  } while($r > 0);

  sigaction(SIGCHLD, $oldact);

  return $ret;
}


sub run_cmd {
  my ($self, $cmd) = (shift, shift);
  my $cmdline = $cmd->cmdline();
  my $env = $cmd->env();
  my $tag = $cmd->tag();
  my $sptr = $self->{'_status'}->{$tag};
  my ($fd, $k, $pid, $r);

  $r = pipe($sptr->{'or'}, $sptr->{'ow'});
  if(!$r) {
    $cmd->failed($!);
    return -1;
  }
  $r = pipe($sptr->{'er'}, $sptr->{'ew'});
  if(!$r) {
    $cmd->failed($!);
    close($sptr->{'or'});
    close($sptr->{'ow'});
    return -1;
  }
  # +O_NONBLOCK, -FD_CLOEXEC
  foreach $fd ($sptr->{'or'}, $sptr->{'ow'}, $sptr->{'er'}, $sptr->{'ew'}) {
    fcntl($fd, F_SETFL, O_NONBLOCK | fcntl($fd, F_GETFL, 0));
    fcntl($fd, F_SETFD, 0);
  }

  $pid = fork();
  if(!defined($pid) || $pid < 0) {
    print(STDERR 'fork failed: ' . $! . "\n");
    $cmd->failed($!);
    close($sptr->{'or'});
    close($sptr->{'ow'});
    close($sptr->{'er'});
    close($sptr->{'ew'});
    return -1;
  } elsif($pid == 0) {
    # child
    foreach $k (keys(%{$env})) {
      $ENV{$k} = $env->{$k};
    }
    open(STDIN, '<', '/dev/null');
    open(STDOUT, '>&', $sptr->{'ow'});
    open(STDERR, '>&', $sptr->{'ew'});
    $fd = select(STDOUT);
    $| = 1;
    select(STDERR);
    $| = 1;
    select($fd);
    close($sptr->{'or'});
    close($sptr->{'er'});
    exec { $cmdline->[0] } (@{$cmdline}); # does not return
  } else {
    $cmd->started();
    $self->{'_status'}->{$tag}->{'pid'} = $pid;
    $self->{'_nfds'}++;
    vec($self->{'_fds'}, fileno($sptr->{'or'}), 1) = 1;
    vec($self->{'_fds'}, fileno($sptr->{'er'}), 1) = 1;
    close($sptr->{'ow'});
    close($sptr->{'ew'});
  }

  return 0;
}


sub collect_output {
  my $self = shift;
  my $fds = $self->{'_fds'};
  my $status = $self->{'_status'};
  my $buf = '';
  my ($cmd, $fd, $k, $tag, $r);

  $r = select($fds, undef, undef, 0.1);
  if($r == -1 || $r == 0) {
    return;
  }

  foreach $tag (keys(%{$status})) {
    $cmd = $self->{'_jobs'}->{$tag};
    foreach $k (qw(or er)) {
      $fd = $self->{'_status'}->{$tag}->{$k};
      if(!(defined($fd) && defined(fileno($fd)))) {
        next;
      }
      if(vec($fds, fileno($fd), 1) == 0) {
        next;
      }
      do {
        $r = sysread($fd, $buf, 8192);
        if(!defined($r) && $! == EAGAIN) {
          next;
        } elsif($r > 0) {
          $cmd->record(substr($k, 0, 1), $buf);
        } else {
          vec($self->{'_fds'}, fileno($fd), 1) = 0;
          close($fd);
          $self->{'_nfds'}--;
        }
      } while(defined($r) && $r > 0);
    }
  }

  return;
}


sub reaper {
  my $self = shift;
  my $sptr = $self->{'_status'};
  my ($cmd, $pid, $tag, $status);

  do {
    $pid = waitpid(-1, WNOHANG);
    if($pid > 0) {
      $status = $?;
      $tag = [
        grep {
          defined($sptr->{$_}->{'pid'}) && $sptr->{$_}->{'pid'} == $pid
        } (keys(%{$sptr}))
      ];
      if(scalar(@{$tag}) == 0) {
        next;
      }
      $cmd = $self->{'_jobs'}->{$tag->[0]};
      $cmd->stopped($status);
      $sptr->{$tag->[0]}->{'pid'} = undef;
    }
  } while($pid > 0);

  return;
}

1;
