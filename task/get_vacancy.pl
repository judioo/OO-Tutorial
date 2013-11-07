#!/usr/bin/env perl
use strict;
use warnings;
use feature qw/say/;
use Data::Dumper;

use FluffyDog::Connect;
use Text::ASCIITable;

use vars qw($dbh);


$dbh = FluffyDog::Connect::connect_live();
if (!$dbh)
{
    die( $DBI::errstr );
}

$dbh->do("alter session set nls_date_format = 'dd-MON-yyyy hh24:mi:ss'");
$dbh->do("alter session set nls_date_language = ENGLISH");


sub return_vacancy_statement_handle {
  my $statement = "SELECT * FROM (SELECT * FROM VACANCY WHERE ROWNUM < 5)";
  my $st_hdl    = $dbh->prepare($statement);
  $st_hdl->execute;

  return $st_hdl;
}

sub display_table {
  my $st_hdl = shift;

  my @required_columns = qw(JOB_TITLE CREATE_DATE CONTRACT_NO);
  my $table = Text::ASCIITable->new({headingText => 'Vacancies For Me'});

  $table->setCols(@required_columns);

  while(my $row = $st_hdl->fetchrow_hashref) {
    $table->addRow(@$row{@required_columns});
  }

  say $table;
}

my $stuff = &return_vacancy_statement_handle;

&display_table($stuff);

