from cloudvolume import CloudVolume
import subprocess
import warnings
from datetime import datetime
from .storage import Storage
import tenacity

CACHE_KWARG = 'cache'

retry = tenacity.retry(
        reraise=True,
        stop=tenacity.stop_after_attempt(7),
        wait=tenacity.wait_full_jitter(0.5, 60.0),
)


class CloudVolumeGSUtil(CloudVolume):

    def __init__(self, *args, **kwargs):
        if CACHE_KWARG in kwargs and not kwargs[CACHE_KWARG]:
            raise ValueError('GSUtil *MUST* use cache')
        super(CloudVolumeGSUtil, self).__init__(cache=True, *args, **kwargs)

    @retry
    def gsutil_download(self, cloudpaths):
        locations = self._compute_data_locations(cloudpaths)

        # load from gsutil only if there is something missing from the cache.
        if len(locations['remote']) != 0:
            gsutil_download_cmd = 'gsutil -m {quiet} cp -I {cache_path}/{key}'.format(
                quiet='' if self.progress else '-q',
                cache_path=self.cache_path,
                key=self.key)

            with Storage(self.layer_cloudpath, progress=self.progress) as storage:
                gspaths = map(storage.get_path_to_file, locations['remote'])

                gcs_pipe = subprocess.Popen([gsutil_download_cmd],
                                                                        stdin=subprocess.PIPE,
                                                                        shell=True)
                gcs_pipe.communicate(input='\n'.join(gspaths))

                if gcs_pipe.returncode:
                    message = 'Error with gsutil transfer. Exit Code {}'.format(
                        gcs_pipe.returncode)
                    raise IOError(message)

    def _fetch_data(self, cloudpaths):
        if self.progress:
            print('Begin download ...')

        start_time = datetime.now()
        try:
            self.gsutil_download(cloudpaths)
        except Exception as e:
            warnings.warn('Error with using gsutil. Message: {}'.format(e))
            warnings.warn('Falling back to default behavior ...')

        end_time = datetime.now()

        if self.progress:
            print('Elapsed Time: {}'.format(end_time - start_time))

        return super(CloudVolumeGSUtil, self)._fetch_data(cloudpaths)
