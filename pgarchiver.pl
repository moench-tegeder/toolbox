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
  my $ret = 0;
  my ($k, $res, $rsync, $runner);

  $rsync = can_run('rsync');
  if(!$rsync) {
    print(STDERR 'cannot find rsync' . "\n");
    return 1;
  }

  $jobinfo = {
    map { $_ => [ $rsync, '-a', $src, $_ ] } (@{$dsts})
  };

  $runner = Internal::Processor->new({'cmd' => $jobinfo});
  $res = $runner->execute();

  foreach $k (keys(%{$res})) {
    if($res->{$k}->{'status'} != 0) {
      if($ret == 0) {
        $ret = $res->{$k}->{'status'};
      }
      print(STDERR 'failed on ' . $k . ': ' . $res->{$k}->{'output'} . "\n");
    }
  }

  return $ret;
}


exit(main());


package Internal::Processor;

use strict;
use warnings;

use IPC::Open3;
use POSIX;
use Time::HiRes qw(gettimeofday tv_interval);


sub new {
  my ($class, $args) = (shift, shift);
  my $self = { %{$args || {}} };

  if(!exists($self->{'cmd'})) {
    return undef;
  }
  $self = bless($self, $class);

  $self->{'__cmdinfo'} = [];
  $self->{'__active'} = {};
  $self->{'__exits'} = {};
  $self->{'__nfds'} = 0;
  $self->{'__fds'} = '';

  return $self;
}


sub execute {
  my $self = shift;
  my $cmds = {};
  my ($i, $ref, $tmp, $r);

  if(ref($self->{'cmd'}) eq 'ARRAY') {
    $cmds = {
      'cmd' => $self->{'cmd'}
    };
  } elsif(ref($self->{'cmd'}) eq 'HASH') {
    $cmds = $self->{'cmd'};
  } else {
    return undef;
  }

  $i = 0;
  foreach $ref (keys(%{$cmds})) {
    $tmp = {
      'tag' => $ref,
      'cmdline' => $cmds->{$ref},
      'idx' => $i,
      'pid' => 0,
      'output' => '',
      'done' => 0,
      'start' => 0,
      'time' => 0,
      'in' => 'in' . $i,
      'out' => 'out' . $i,
      'err' => 'err' . $i
    };
    push(@{$self->{'__cmdinfo'}}, $tmp);
    $i++;
  }

  $r = $self->run();

  return $r;
}


sub run {
  my ($self, $cmds) = (shift, shift);
  my ($cmd, $cmdline, $lvl, $pid, $ref, $ret, $status, $tmp);
  my ($act, $oldact, $oldset, $set, $setchld);

  $oldset = POSIX::SigSet->new();
  $set = POSIX::SigSet->new(SIGTERM, SIGINT);
  $act = POSIX::SigAction->new(sub { $self->reaper() }, $set);
  $act->safe(1);
  $oldact = POSIX::SigAction->new();
  sigaction(SIGCHLD, $act, $oldact);

  foreach $cmd (@{$self->{'__cmdinfo'}}) {
    $self->run_cmd($cmd);
  }

  while($self->{'__nfds'} > 0) {
    $self->collect_output();
  }

  while(kill(0, (keys(%{$self->{'__active'}})))) {
    sleep(1);
  }
  
  sigprocmask(SIG_BLOCK, $setchld);
  foreach $pid (keys(%{$self->{'__exited'}})) {
    $cmd = $self->{'__cmdinfo'}->[$self->{'__active'}->{$pid}];
    $status = $self->{'__exited'}->{$pid}->{'status'};
    if($status >= 0 && WIFEXITED($status)) {
      $cmd->{'status'} = WEXITSTATUS($status);
    } else {
      $cmd->{'status'} = -1;
    }
    if($status >= 0 && WIFSIGNALED($status)) {
      $cmd->{'signal'} = WTERMSIG($status);
    } else {
      $cmd->{'signal'} = 0;
    }
    if($status < 0) {
      $cmd->{'status'} = -1;
      $cmd->{'signal'} = 0;
    }
    $cmd->{'time'} = $self->{'__exited'}->{$pid}->{'time'};
    delete($self->{'__active'}->{$pid});
  }
  $self->{'__exited'} = {};

  sigprocmask(SIG_SETMASK, $oldset);
  sigaction(SIGCHLD, $oldact);

  $ret = {
    map {
      $_->{'tag'} => {
        'status' => $_->{'status'},
        'signal' => $_->{'signal'},
        'time' => $_->{'time'},
        'output' => $_->{'output'}
      }
    } (@{$self->{'__cmdinfo'}})
  };

  return $ret;
}


sub run_cmd {
  my ($self, $cmd) = (shift, shift);
  my ($cmdline, $flags, $pid);
  $cmdline = $cmd->{'cmdline'};

  eval {
    $pid = open3($cmd->{'in'}, $cmd->{'out'}, $cmd->{'err'}, @{$cmdline});
  };
  if($@ || $pid < 0) {
    print(STDERR 'command ' . $cmd->{'tag'} . ' failed: ' . ($@ ? $@ : $!));
    $cmd->{'output'} = $@ ? $@ : $!;
    $cmd->{'status'} = -1;
    $cmd->{'done'} = 1;
    return -1;
  }

  $cmd->{'pid'} = $pid;
  $cmd->{'status'} = undef;
  $cmd->{'start'} = [ gettimeofday() ];

  $flags = fcntl($cmd->{'out'}, F_GETFL, 0);
  fcntl($cmd->{'out'}, F_SETFL, $flags | O_NONBLOCK);
  $flags = fcntl($cmd->{'err'}, F_GETFL, 0);
  fcntl($cmd->{'err'}, F_SETFL, $flags | O_NONBLOCK);

  vec($self->{'__fds'}, fileno($cmd->{'out'}), 1) = 1;
  vec($self->{'__fds'}, fileno($cmd->{'err'}), 1) = 1;
  $self->{'__nfds'} += 2;;

  $self->{'__active'}->{$pid} = $cmd->{'idx'};

  return 0;
}


sub collect_output {
  my $self = shift;
  my $fds = $self->{'__fds'};
  my $buf = '';
  my ($c, $fd, $r);

  $r = select($fds, undef, undef, 0.1);
  if(!$r) {
    return;
  }

  foreach $c (@{$self->{'__cmdinfo'}}) {
    foreach $fd ($c->{'out'}, $c->{'err'}) {
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
          $c->{'output'}.= $buf;
          $buf = '';
        } else {
          vec($self->{'__fds'}, fileno($fd), 1) = 0;
          close($fd);
          $self->{'__nfds'}--;
        }
      } while(defined($r) && $r > 0);
    }
  }

  return;
}


sub reaper {
  my $self = shift;
  my ($idx, $pid);

  do {
    $pid = waitpid(-1, WNOHANG);
    if($pid > 0) {
      $idx = $self->{'__active'}->{$pid};
      $self->{'__exited'}->{$pid} = {
        'status' => $?,
        'time' => tv_interval($self->{'__cmdinfo'}->[$idx]->{'start'})
      };
    }
  } while($pid > 0);

  return;
}

1;
