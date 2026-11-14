# Setup: Mac and Windows

Everything runs inside a Docker container: Hadoop (HDFS and YARN), Hive, Spark, Kafka and HBase on Ubuntu with Java 8. Your own computer only needs Docker and Git. The repo folder is linked into the container, so files you change on your computer appear inside it straight away.

The setup takes about 30 to 60 minutes once, mostly downloads. After that, starting work takes three commands.

> **Status:** the `Dockerfile` route below has not been test-built yet: the project's runs used an equivalent image built earlier in the semester on a Mac. If a step fails, send the error to the group.

## What you need

| | Requirement |
| --- | --- |
| Docker Desktop | [docker.com/products/docker-desktop](https://www.docker.com/products/docker-desktop/). On Windows it uses WSL 2; the installer sets this up |
| Git | Mac: built in (type `git` in Terminal and accept the install prompt). Windows: [git-scm.com](https://git-scm.com/download/win) |
| Memory | At least 8 GB of RAM available to Docker (Docker Desktop, Settings, Resources) |
| Disk | About 10 GB free |

Commands below are typed in **Terminal** on a Mac and in **PowerShell** on Windows, unless a step says "inside the container". Inside the container it is the same Linux on both.

## 1. Get the repo

Mac (Terminal):

```
mkdir -p ~/repositories
cd ~/repositories
git clone https://github.com/mwlde/DEWA-Smart-Meter-Data-Lake.git
cd DEWA-Smart-Meter-Data-Lake
```

Windows (PowerShell):

```
mkdir $HOME\repositories -Force
cd $HOME\repositories
git clone https://github.com/mwlde/DEWA-Smart-Meter-Data-Lake.git
cd DEWA-Smart-Meter-Data-Lake
```

Clone the repo rather than downloading a zip: the repo's `.gitattributes` file keeps the scripts' Linux line endings, and scripts with Windows line endings fail inside the container.

## 2. Build the image (once)

Open Docker Desktop and wait until it says it is running. Then, from the repo folder (same command on both):

```
docker build -t dewa-hadoop .
```

This downloads Ubuntu, Java 8 and Hadoop (about 1 GB) and takes 5 to 15 minutes.

## 3. Create the container (once)

Mac:

```
docker run -d --name hadoop -p 8088:8088 -p 9870:9870 -p 19888:19888 -v "$PWD":/home/hadoop/dewa dewa-hadoop
```

Windows (PowerShell):

```
docker run -d --name hadoop -p 8088:8088 -p 9870:9870 -p 19888:19888 -v "${PWD}:/home/hadoop/dewa" dewa-hadoop
```

The `-v` part links the repo folder to `/home/hadoop/dewa` inside the container. Run this from the repo folder, or the link points at the wrong place.

Check it is running:

```
docker ps
```

`STATUS` should say `Up`. Give Hadoop about 30 seconds to start.

## 4. Install the project tools (once)

Go inside the container (same on both):

```
docker exec -it hadoop bash
```

The prompt changes to `hadoop@...:~$`. Now:

```
bash /home/hadoop/dewa/scripts/setup_container.sh
```

This downloads and configures Hive, Spark, Kafka and HBase (about 1.2 GB, 10 to 30 minutes depending on your connection). Java 8 is already in the image, so skip the root command mentioned at the top of that script.

When it says `Setup finished`:

```
exit
docker restart hadoop
docker exec -it hadoop bash
bash /home/hadoop/dewa/scripts/check_tools.sh
```

You should see Java 1.8, `OK` next to all five tools, and the Hadoop daemons (NameNode, DataNode, ResourceManager, NodeManager). Setup is done; the README explains how to run the pipeline.

## Every session after that

```
docker start hadoop
docker exec -it hadoop bash
cd /home/hadoop/dewa
```

Docker Desktop must be open first. When you finish: `exit`, then `docker stop hadoop`.

**Never run `docker rm hadoop`.** It deletes the container with all its installs and the data in HDFS. Stopping is safe; the repo itself is always safe on your computer.

## Web pages (optional)

While the container runs, these open in your browser:

| Page | Address |
| --- | --- |
| HDFS (browse files under Utilities) | http://localhost:9870 |
| YARN (running and finished jobs) | http://localhost:8088 |
| Job History | http://localhost:19888 |

## Troubleshooting

| Problem | Fix |
| --- | --- |
| `failed to connect to the docker API` or `Cannot connect to the Docker daemon` | Docker Desktop is not running. Open it, wait until it is ready, try again |
| `docker: command not found` | You typed a `docker` command inside the container. `docker` commands go on your computer (Terminal or PowerShell), not at the `hadoop@...` prompt |
| `$'\r': command not found` or `bad interpreter` when running a script | The script has Windows line endings. Inside the container: `sed -i 's/\r$//' scripts/*.sh`, then run it again. Cloning with Git (not a zip download) prevents this |
| `port is already allocated` on `docker run` | Another container uses the same ports. `docker ps -a` to find it, stop it with `docker stop NAME` |
| `No such container: hadoop` | The container was never created or was removed. Repeat step 3 (and step 4) |
| Daemons missing from `check_tools.sh` | Inside the container: `bash /home/hadoop/start.sh &`, wait 30 seconds, check again |
| A download fails during `setup_container.sh` | Run it again: finished downloads are kept. If it still fails, send the error; the version number at the top of the script may need changing |
| Everything is very slow, or jobs are killed | Give Docker more memory (Docker Desktop, Settings, Resources), at least 8 GB |
| Windows: the repo folder is slow inside the container | Normal for folders on the Windows drive. Optional: clone the repo inside WSL (`\\wsl$\Ubuntu\home\...`) and run steps 3 and 4 from there |


