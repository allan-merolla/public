add-pssnapin microsoft.sharepoint.powershell
start-spassignment -global
function fixLists($web)
{
	$webLists = ($web.Lists | ? {$_.BaseType -like "DocumentLibrary*"} | ? {$_.EnableVersioning -eq $true} )
	foreach ($l in $webLists) {
		Write-Host "Updating List: " $l.Title
		$l.EnableVersioning= $false
		$l.Update()
		$l.EnableVersioning = $true
		$l.Update()
		Write-Host $l.Title " - List Updated..."
	}
}
function fixWebs($web)
{
	Write-Host "Updating web: " $web.Title
	fixLists($web)
	foreach ($w in $web.Webs) {fixWebs($w)}
}
Write-Host "Start List Update..."
$site = get-spsite http://sitename
fixWebs($site.RootWeb)

stop-spassignment -global
