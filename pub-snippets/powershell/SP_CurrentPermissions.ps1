Add-PSSnapin Microsoft.SharePoint.Powershell
$web = get-spweb https://XXXXX.XXXX.XXXXX/project/XXXXXX
$library = $web.Lists["Project Documents"]
$output = @()
$users = $web.SiteUsers
foreach ($user in $users) {
# Set the users login name
$loginName = $user
if ($user -is [Microsoft.SharePoint.SPUser] -or $user -is [PSCustomObject]) {
$loginName = $user.LoginName
}
if ($loginName -eq $null) {
throw "The provided user is null or empty. Specify a valid SPUser object or login name."
}
# G et the users permission details.
$siteperms = $library.GetUserEffectivePermissionInfo($loginName)
# Determine the URL to the securable object being evaluated
$siteObject = $null
if ($library -is [Microsoft.SharePoint.SPWeb]) {
$siteObject = $library.Url
} elseif ($library -is [Microsoft.SharePoint.SPList]) {
$siteObject = $library.ParentWeb.Site.MakeFullUrl($library.RootFolder.ServerRelativeUrl)
} elseif ($library -is [Microsoft.SharePoint.SPListItem]) {
$siteObject = $library.ParentList.ParentWeb.Site.MakeFullUrl($library.Url)
}
# Get the role assignments and iterate through them
$roleAssignments = $siteperms.RoleAssignments
if ($roleAssignments.Count -gt 0) {
foreach ($roleAssignment in $roleAssignments) {
$memb = $roleAssignment.Member
# Build a string array of all the permission level names
$permName = @()
foreach ($definition in $roleAssignment.RoleDefinitionBindings) {
$permName  = $definition.Name
}
# Determine how the users permissions were assigned
$assignment = "Direct Assignment"
if ($memb -is [Microsoft.SharePoint.SPGroup]) {
$assignment = $memb.Name
} else {
if ($memb.IsDomainGroup -and ($memb.LoginName -ne $loginName)) {
$assignment = $memb.LoginName
}
}
# Create a hash table with all the data
$obj = new-object -TypeName PSObject
$obj | add-member -MemberType NoteProperty -Name Resource -Value $siteObject
$obj | add-member -MemberType NoteProperty -Name ResourceType -Value $library.GetType().Name
$obj | add-member -MemberType NoteProperty -Name User -Value $loginName
$obj | add-member -MemberType NoteProperty -Name Permission -Value $permName.ToString()
$obj | add-member -MemberType NoteProperty -Name GrantedVia -Value $assignment
$output += $obj

}
}
}
$output | sort-object User | ? {$_.GrantedVia -notmatch "Style Resource Readers"}| ? {$_.User -notmatch "_wa"} | export-csv C:\users\svc_sp_install\Desktop\XXXXX_XXXXXX.csv