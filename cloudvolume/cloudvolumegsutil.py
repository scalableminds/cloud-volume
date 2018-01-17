from cloudvolume import CloudVolume
import subprocess
import sys
from datetime import datetime
from .storage import Storage


CACHE_KWARG = 'cache'

class CloudVolumeGSUtil(CloudVolume):
  def __init__(self, *args, **kwargs):
    if CACHE_KWARG in kwargs and not kwargs[CACHE_KWARG]:
      raise ValueError("GSUtil *MUST* use cache")
    super(CloudVolumeGSUtil, self).__init__(cache=True, *args, **kwargs)

  def _fetch_data(self, cloudpaths):

    locations = self._compute_data_locations(cloudpaths)

    # if there is something missing from the cache, load from gsutil
    if len(locations['remote']) != 0:
      gsutil_download_cmd = "gsutil -m {quiet} cp -I {cache_path}/{key}".format(
        quiet='' if self.progress else '-q',
        cache_path=self.cache_path,
        key=self.key)

      gcs_pipe = subprocess.Popen([gsutil_download_cmd], stdin=subprocess.PIPE,
        shell=True)

      with Storage(self.layer_cloudpath, progress=self.progress) as storage:
        gspaths = map(storage.get_path_to_file, locations['remote'])

        start_time = datetime.now()
        gcs_pipe.communicate(input="\n".join(gspaths))
        end_time = datetime.now()

        if gcs_pipe.returncode:
          raise IOError("Something went wrong with the gsutil transfer.")

        if self.progress:
            print("Elapsed Time: {}".format(end_time - start_time))

    return super(CloudVolumeGSUtil, self)._fetch_data(cloudpaths)
