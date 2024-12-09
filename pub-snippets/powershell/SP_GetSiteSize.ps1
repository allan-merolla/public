# Server Side Object Model
Add-PSSnapin microsoft.sharepoint.powershell
$SizeLog = "D:\temp\SPSiteSize.csv"
$CurrentDate = Get-Date -format d
$WebApps = Get-SPWebApplication
foreach($WebApp in $WebApps)
{
	$Sites = Get-SPSite -WebApplication $WebApp -Limit All
	foreach($Site in $Sites)
	{
	$SizeInKB = $Site.Usage.Storage
	$SizeInGB = $SizeInKB/1024/1024/1024
	$SizeInGB = [math]::Round($SizeInGB,2)
	$CSVOutput = $Site.RootWeb.Title + "*" + $Site.URL + "*" + $Site.ContentDatabase.Name + "*" + $SizeInGB + "*" + $CurrentDate
	$CSVOutput | Out-File $SizeLog -Append
	}
}
$Site.Dispose()