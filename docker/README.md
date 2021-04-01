# UFDL Job Launcher

Docker image for running the UFDL job launcher.
 

## Docker

### Quick start

* Log into registry using *public* credentials:

  ```commandline
  docker login -u public -p public public.aml-repo.cms.waikato.ac.nz:443 
  ```

* Pull and run image (adjust volume mappings `-v`):

  ```commandline
  docker run \
    --net=host \
    -v /var/run/docker.sock:/var/run/docker.sock \
    -v /local/dir/ufdl-job-launcher/job-launcher.conf:/ufdl/ufdl-job-launcher/examples/job-launcher-example.conf \
    -v /tmp/ufdl-job-launcher:/tmp/ufdl-job-launcher \
    -it public.aml-repo.cms.waikato.ac.nz:443/ufdl/ufdl_job_launcher:latest
  ```

* If need be, remove all containers and images from your system:

  ```commandline
  docker stop $(docker ps -a -q) && docker rm $(docker ps -a -q) && docker system prune -a
  ```


### Build local image

* Build the image from Docker file (from within /path_to/ufdl/image_classification/docker/1.14)

  ```commandline
  docker build -t ufdl_job_launcher .
  ```

* Run the container

  ```commandline
  docker run \
    --net=host \
    -v /var/run/docker.sock:/var/run/docker.sock \
    -v /local/dir/ufdl-job-launcher/job-launcher.conf:/ufdl/ufdl-job-launcher/examples/job-launcher-example.conf \
    -v /tmp/ufdl-job-launcher:/tmp/ufdl-job-launcher \
    -it ufdl_job_launcher:latest
  ```
  * `-p X:Y` maps local port X to container port Y 
  * `-v /local/dir:/container/dir` maps a local disk directory into a directory inside the container
    (only maps files if the file already exists inside the container)

### Pre-built images

* Build

  ```commandline
  docker build -t ufdl/ufdl_job_launcher:latest .
  ```
  
* Tag

  ```commandline
  docker tag \
    ufdl/ufdl_job_launcher:latest \
    public-push.aml-repo.cms.waikato.ac.nz:443/ufdl/ufdl_job_launcher:latest
  ```
  
* Push

  ```commandline
  docker push public-push.aml-repo.cms.waikato.ac.nz:443/ufdl/ufdl_job_launcher:latest
  ```
  If error "no basic auth credentials" occurs, then run (enter username/password when prompted):
  
  ```commandline
  docker login public-push.aml-repo.cms.waikato.ac.nz:443
  ```
  
* Pull

  If image is available in aml-repo and you just want to use it, you can pull using following command and then [run](#run).

  ```commandline
  docker pull public.aml-repo.cms.waikato.ac.nz:443/ufdl/ufdl_job_launcher:latest
  ```
  If error "no basic auth credentials" occurs, then run (enter username/password when prompted):
  
  ```commandline
  docker login public.aml-repo.cms.waikato.ac.nz:443
  ```
  Then tag by running:
  
  ```commandline
  docker tag \
    public.aml-repo.cms.waikato.ac.nz:443/ufdl/ufdl_job_launcher:latest \
    ufdl/ufdl_job_launcher:latest
  ```

* <a name="run">Run</a>

  ```commandline
  docker run \
    --net=host \
    -v /var/run/docker.sock:/var/run/docker.sock \
    -v /local/dir/ufdl-job-launcher/job-launcher.conf:/ufdl/ufdl-job-launcher/examples/job-launcher-example.conf \
    -v /tmp/ufdl-job-launcher:/tmp/ufdl-job-launcher \
    -it ufdl/ufdl_job_launcher:latest
  ```
  * `-p X:Y` maps local port X to container port Y 
  * `-v /local/dir:/container/dir` maps a local disk directory into a directory inside the container
    (only maps files if the file already exists inside the container)
