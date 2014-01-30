#!/usr/bin/perl
use strict;
use warnings;
use Getopt::Long;
use Pod::Usage;
use RedisDB;
use File::Find;
use JSON::XS;
use Digest::MD5 qw( md5_hex );
use Compress::Raw::Bzip2;
use Crypt::CBC;
use Sys::Hostname;


=head1 NAME

populate_mps_cache.pl - read /var/cfengine/outgoing and store it in RedisDB

=head1 DESCRIPTION

read files in /var/cfengine/outgoing and store data in RedisDB based 

=head1 SYNOPSIS

=head1 SUBS

=head2 main

=cut

sub main {
    my ( $help, $man, $verbose );

    my @list_of_files;
    my ( $filecontent, $filecontentcompressed, @wc_words );
    my $hostname = hostname();
    my $outgoingdir = "/var/cfengine/outgoing";
    my @mps;

    my $cipher = Crypt::CBC->new( -key => '${shared_global_environment.sysopskey}',
                                  -cypher => 'DES_EDE3'
                                );

    my $result = GetOptions (
        "help|h"    => \$help,
        "verbose|v"   =>  \$verbose,
        "man|m"      => \$man
        );

    open CM, "/etc/cm.conf" or die "couldn't find cm servers";

    foreach my $line ( <CM> )
      {
        if ( $ line =~ m/\w+_MPS:(\d+\.\d+.\d+\.\d+)/ )
          {
            push @mps, $1;
          }
      }
    close CM;

    if ( $man ) { pod2usage( -verbose => 2 ); }

    if ( $help )
    {
        pod2usage(1);
    }

    opendir( DIR, $outgoingdir) or die __LINE__ . " could not search for files";
    my @outgoingcontent = readdir( DIR );
    closedir( DIR );

    foreach my $file (@outgoingcontent)
      {
        next if ($file eq ".");
        next if ($file eq "..");

        if ( -l "$outgoingdir/$file" )
          {
            if ( -d ( readlink( "$outgoingdir/$file" ) ) )
              {

                find(
                     {
                      wanted => sub {
                        if ( -f $File::Find::name && -r $File::Find::name )
                          {
                            push @list_of_files, $File::Find::name;
                          }
                        },
                      follow => 1,
                      follow_skip => 2,
                     },
                     ( readlink( "$outgoingdir/$file" ) )
                    );

              } elsif ( -f (readlink ( "$outgoingdir/$file" ) ) )
                {
                  push @list_of_files, readlink( "$outgoingdir/$file" );
                }

          } elsif ( -f $file && -r $file )
            {
              push @list_of_files, $file;
            }
      }

    foreach my $mpsserver (@mps)
      {
        if ( $verbose ) { print $mpsserver . "\n"; };

        my $redis = RedisDB->new( host => "$mpsserver",
                                  port => 6379,
                                  database => 1
                                );

        foreach my $file ( @list_of_files )
          {
            if ( -f $file && -r $file )
              {
                $filecontent = "";
                $filecontentcompressed = "";

                open FILE, $file or die __LINE__ . "couldn't open $file\n";
                my $result = sysread ( FILE, $filecontent, "10485760" );        # 10485760 Byte = 10 MB
                close FILE;

                my ($bz, $status) = new Compress::Raw::Bzip2 1, 9, 30
                  or die "Cannot create bzip2 object\n";

                $status = $bz->bzdeflate( $cipher->encrypt( $filecontent ), $filecontentcompressed );
                $status = $bz->bzflush( $filecontentcompressed );
                $status = $bz->bzclose( $filecontentcompressed );

                @wc_words = split(/[ \t\n]+/, $filecontent);

                my $json = { content => $filecontentcompressed,
                             md5sum => md5_hex( $filecontent ),
                             osstat => [ stat( $file ) ],
                             wc => "Number of characters: " .  (length( $filecontent) ) . " Number of lines: " . ( $filecontent =~ tr/\n// ) . " Number of words: " . @wc_words,
                           };

                $redis->set( $hostname . '#' . $file, encode_json $json);
                $redis->expire( $hostname . '#' . $file, 2592000 ); # 2592000 = 30 days
              }
          }
      }
}


main();

=head1 AUTHOR


 <baron@bundesbrandschatzamt.de>

=head1 LICENSE

This program is free software, you can redistribute it and/or modify
it under the same terms as Perl itself.

=cut

1;
