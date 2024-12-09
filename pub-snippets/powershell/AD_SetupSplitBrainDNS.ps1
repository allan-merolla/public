Add-DnsServerPrimaryZone -Name "examples" -ReplicationScope "Forest" -PassThru
Add-DnsServerZoneScope -ZoneName "examples" -Name "internal"
Add-DnsServerResourceRecordCName -Name "apps" -HostNameAlias "app.online.com.au" -ZoneName "examples" -ZoneScope "internal"
Add-DnsServerResourceRecordCName -Name "dashboard" -HostNameAlias "xx.amazonaws.com" -ZoneName "examples" -ZoneScope "internal"
Add-DnsServerResourceRecordCName -Name "www" -HostNameAlias "examples" -ZoneName "examples" -ZoneScope "internal"
Add-DnsServerResourceRecord -ZoneName "examples" -A -Name "examples.news" -IPv4Address "203.1.1.1"  -ZoneScope "internal"
#repeat on each DC
Set-DnsServerClientSubnet -Name "ADTrustZone" -Action ADD -IPv4Subnet "10.0.0.0/8" -PassThru

Add-DnsServerQueryResolutionPolicy -Name "SplitBrainZonePolicy" -Action ALLOW -ClientSubnet "EQ,ADTrustZone" -ZoneScope "internal,1" -ZoneName "examples"

Get-DnsServerClientSubnet
Get-DnsServerResourceRecord -ZoneName "examples"
Get-DnsServerResourceRecord -ZoneName "examples" -ZoneScope "internal"
Get-DnsServerQueryResolutionPolicy -ZoneName "examples"
