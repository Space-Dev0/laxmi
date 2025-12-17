ssh -t -i "laxmi.pem" ubuntu@ec2-52-205-10-59.compute-1.amazonaws.com "screen -S laxmi_bot bash -l -c 'cd laxmi/; micro config.yaml; uv run main.py; exec bash'"
