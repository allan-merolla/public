    #Load SharePoint Snap In
    Add-PSSnapin Microsoft.SharePoint.PowerShell -ErrorAction SilentlyContinue

    function Add-SPPermissionToListItemUserConditional
    {
     param ($Url, $ListName, $UserName, $Caml, $PermissionLevel)
     $web = Get-SPWeb -Identity $Url
     $list = $web.Lists.TryGetList($ListName)
     if ($list -ne $null)
   {
           $spQuery = New-Object Microsoft.SharePoint.SPQuery
            $spQuery.Query = $Caml
            $spQuery.RowLimit = 10000
            $listItems = $list.GetItems($spQuery)
          $listItems.Count
         foreach ($item in $listItems)
        {
        write-host $item.ID
               if ($item.HasUniqueRoleAssignments -eq $true)

             {
                    $user = $web.EnsureUser($UserName)
                  $roleDefinition = $web.RoleDefinitions[$PermissionLevel]
                    $roleAssignment = New-Object Microsoft.SharePoint.SPRoleAssignment($user)
                   $roleAssignment.RoleDefinitionBindings.Add($roleDefinition)
                  $item.RoleAssignments.Add($roleAssignment)
                 $item.Update()
                       Write-Host "Successfully added $PermissionLevel permission to $UserName in $ListName list. " -foregroundcolor Green

             }
           }

     }

     $web.Dispose()
  }

#Add-SPPermissionToListItemUserConditional "https://sharepointsite" "Library" "AD\ADD_THIS_ADGROUP" "<Where><Eq><FieldRef Name='Status' /><Value Type='Choice'>Approved</Value></Eq></Where>" "Read"
