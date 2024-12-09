#Updates Welcome Pages for Web Urls as per supplied CSV

Add-PSSnapin Microsoft.SharePoint.PowerShell
D:
CD D:\Temp
$WelcomePages = @()
$WelcomePages | Import-CSV "XXXXXX.csv"

$WelcomePages | % {
	$web = $_.Web -replace "/XXXXXXXXXX/", "/XXXXXXX/"
	$web = $_.Web -replace "/X/", "/XXXXXXX/"
	write-host "WEB: " $web
	$Site = Get-SPWeb -identity $web
	$RootFolder = $Site.RootFolder
	write-host "Old HomePage: " $Site.WelcomePage
	write-host "New HomePage: " $_.WelcomePage
	$RootFolder.WelcomePage = $_.WelcomePage
	$RootFolder.Update(); 
	$Site.Dispose()
	$_.Dispose()
	break
}
	
$WelcomePages = @()
