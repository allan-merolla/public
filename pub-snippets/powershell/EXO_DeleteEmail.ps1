   1 Import-Module ExchangeOnlineManagement
   2 install-module exchangeonlinemanagement
   3 Connect-IPPSSession -UserPrincipalName allan.merolla@turnkey.global
  11 New-ComplianceSearch -Name "Sherif" -ExchangeLocation all -ContentMatchQuery '(subject:"Goodbye") AND (from:theshirif@example.com)'
  13 Start-ComplianceSearch Sherif
  16 Get-ComplianceSearch Sherif | fl -Property Items
  17 New-ComplianceSearchAction -SearchName Sherif -Purge -PurgeType SoftDelete
  26 Get-ComplianceSearchAction Sherif_Purge | fl -Property Results