$webUrl = "http://XXXX/XXXX/XXXX-XXXXX";

$web = get-spweb $webUrl;
$documentContentType = $web.ContentTypes |? {$_.Name -eq "XXXXX Document Report"}

$fieldNames = @();
$fieldNames += "RTIO_OpReports_ReportPreviewImage";
write-host $documentContentType.Name;
foreach ($fieldName in $fieldNames)
{
  write-host $fieldName;
  $field = $documentContentType.Fields |? {$_.InternalName -eq "XXXXX_XXXXX_ReportPreviewImage"};
  write-host $field.Title;
  $field.ShowInDisplayForm = $false;
  $field.ShowInViewForms = $false;
  $documentContentType.Update($true);
}
