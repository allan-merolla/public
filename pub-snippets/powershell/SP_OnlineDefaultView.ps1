#### CONFIGURABLE START #####

#Specify tenant admin and site URL
$User = "user@domain.com.au"
$Password = ConvertTo-SecureString "Welcome321" -asplaintext -force
$SiteURL = "https://abc.sharepoint.com/sites/abc"
$ListTitle = "Documents"


#### CONFIGURABLE END #####



#Add references to SharePoint client assemblies and authenticate to Office 365 site - required for CSOM
Add-Type -Path "C:\Program Files\SharePoint Client Components\Assemblies\Microsoft.Online.SharePoint.Client.Tenant.dll"
Add-Type -Path "C:\Program Files\Common Files\microsoft shared\Web Server Extensions\15\ISAPI\Microsoft.SharePoint.Client.dll"
Add-Type -Path "C:\Program Files\Common Files\microsoft shared\Web Server Extensions\15\ISAPI\Microsoft.SharePoint.Client.Runtime.dll"

[Net.ServicePointManager]::SecurityProtocol = [Net.SecurityProtocolType]::Tls12
$Creds = New-Object Microsoft.SharePoint.Client.SharePointOnlineCredentials($User,$Password)
#Bind to site collection
$Context = New-Object Microsoft.SharePoint.Client.ClientContext($SiteURL)
$Context.Credentials = $Creds
#Retrieve lists
$List= $Context.Web.Lists.GetByTitle($ListTitle)
$Context.Load($List)
$Context.ExecuteQuery()

$DefaultView = $List.DefaultView
$DefaultView.ViewQuery = '<OrderBy><FieldRef Name="FileLeafRef" /></OrderBy><Where><Neq><FieldRef Name="Archive" /><Value Type="Boolean">1</Value></Neq></Where>'# <GroupBy Collapse="TRUE" GroupLimit ="1000"><FieldRef Name="ContentType"/></GroupBy>
$DefaultView.Update()
$Context.ExecuteQuery()

#$Context.Load($List.Views)
#$Context.ExecuteQuery()
#$view =  $List.Views | ?{$_.title -eq "All Documents"}
#$Context.Load($view)
#$Context.ExecuteQuery()
#$view.ViewQuery =
#$view.Update()
#$List.Update()

#Read more: http://www.sharepointdiary.com/2012/07/set-sorting-filter-groupby-in-sharepoint-views-programmatically.html#ixzz56Re5jOeE

#Create list with "custom" list template
##$ListInfo = New-Object Microsoft.SharePoint.Client.ListCreationInformation
##$ListInfo.Title = $ListTitle
##$ListInfo.TemplateType = "100"
##$List = $Context.Web.Lists.Add($ListInfo)
##$List.Description = $ListTitle
##$List.Update()
##$Context.ExecuteQuery()

#Retrieve site columns (fields)
##$SiteColumns = $Context.Web.AvailableFields
##$Context.Load($SiteColumns)
##$Context.ExecuteQuery()

#Grab city and company fields
##$City = $Context.Web.AvailableFields | Where {$_.Title -eq "City"}
##$Company = $Context.Web.AvailableFields | Where {$_.Title -eq "Company"}
##$Context.Load($City)
##$Context.Load($Company)
##$Context.ExecuteQuery()

#Add fields to the list
##$List.Fields.Add($City)
##$List.Fields.Add($Company)
##$List.Update()
##$Context.ExecuteQuery()

#Add fields to the default view
##$DefaultView = $List.DefaultView
##$DefaultView.ViewFields.Add("City")
##$DefaultView.ViewFields.Add("Company")
##$DefaultView.Update()
##$Context.ExecuteQuery()

#Adds an item to the list
##$ListItemInfo = New-Object Microsoft.SharePoint.Client.ListItemCreationInformation
##$Item = $List.AddItem($ListItemInfo)
##$Item["Title"] = "New Item"
##$Item["Company"] = "Contoso"
##$Item["WorkCity"] = "London"
##$Item.Update()
##$Context.ExecuteQuery()