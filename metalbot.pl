#!/usr/bin/perl

#create table deathmetal( id serial UNIQUE NOT NULL, line text not null, primary key (id));

use strict;
use warnings;
use POSIX;
use Net::Twitter;
use Net::Twitter::Diff;
use Net::Twitter::Search;
use Data::Dumper;
use Unicode::String qw(utf8);
use DBI;


my $dsn    = "dbi:Pg:dbname=bots";
my $dbh    = DBI->connect($dsn, 'brenda', '') or unlock_die ("Cannot connect to the database '$dsn': $DBI::errstr");


readInputFiles();

my $username='metalbot';
my $password = 'brillig'; 

my $twit = Net::Twitter->new(  username => $username, password => $password);

my @result = getRecordSet('SELECT count(*) as number FROM deathmetal');
my $max = $result[0]->{'number'};

print "Max = $max\n"; 

if ($max < 3) {
  $twit->update('d br3nda Only ' . $max .' lines of death metal left') or die;
}
my $random_id = ceil(rand($max));
print "using line $random_id\n";

@result = getRecordSet("SELECT id, line from deathmetal ORDER BY id OFFSET $random_id LIMIT 1");
my $line = $result[0]->{'line'};
my $id = $result[0]->{'id'};
print "line = $line\n";
$twit->update($line);

doSql("DELETE FROM deathmetal WHERE id=" . $id);


sub getRecordSet {
    my $query = shift;
    print "$query\n";
    my $sQuery = $dbh->prepare($query);
    $sQuery->execute or die ("Error in query:\n $query\n");

    my $hRecordSet;
    my @aReturnRecordSet;
    while ( my $sRecordSet = $sQuery->fetchrow_hashref()) {
        push(@aReturnRecordSet,$sRecordSet);
    }

    return (@aReturnRecordSet);
}
sub doSql {
    my $query = shift;
    print "$query\n";
    my $sQuery = $dbh->prepare($query);
    $sQuery->execute or die ("Error in query:\n $query\n");

}
sub readInputFiles {
  print "Checking for incoming DEATH METAL \\m/\n";
  use File::stat;
  my $file = '/home/brenda/projects/twitter/metalbot/incoming/data.txt';
  if (! -e $file) {
    print "no input\n";
 	return;
  } 
  open(INFO, "<$file");
  my @lines = <INFO>;
  close(INFO) ;  

  foreach my $line (@lines) {
	chomp($line);
	$line =~ s/^\s+//;
	$line =~ s/\s+$//;
	next if ($line eq '');
 	next if ($line =~ m/:/);
	print "$line\n";
	$line =~ s/'/''/g;
	doSql("INSERT INTO deathmetal (line) VALUES ('$line')");
  }
  use File::Copy;
  my $newfile = '/home/brenda/projects/twitter/megahal/incoming/processed/';
  die unless move ($file, $newfile);
}
