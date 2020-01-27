FROM dockcross/linux-x64

ENV DEFAULT_DOCKCROSS_IMAGE raft-server-erlang-build-image
RUN wget https://packages.erlang-solutions.com/erlang-solutions_1.0_all.deb && sudo dpkg -i erlang-solutions_1.0_all.deb && rm erlang-solutions_1.0_all.deb
RUN curl -sL https://deb.nodesource.com/setup_11.x | sudo bash -
RUN sudo apt-get -y install esl-erlang=1:21.1
RUN wget https://s3.amazonaws.com/rebar3/rebar3 && chmod +x rebar3 && mv rebar3 /usr/local/bin/.
RUN sudo apt-get -y install nodejs
RUN sudo apt-get -y install default-jre
RUN sudo npm install node-plantuml -g
RUN sudo apt-get -y install graphviz
