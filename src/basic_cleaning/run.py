#!/usr/bin/env python
"""
Download from W&B the raw dataset and apply some basic data cleaning, exporting the result to a new artifact
"""
import argparse
import logging
import wandb
import pandas as pd


logging.basicConfig(level=logging.INFO, format="%(asctime)-15s %(message)s")
logger = logging.getLogger()


def go(args):

    # Initializing
    run = wandb.init(job_type="basic_cleaning")
    run.config.update(args)
    logger.info("Downloading input from W&B")
    local_path = wandb.use_artifact(args.input_artifact).file()
    df = pd.read_csv(local_path)

    # Cleaning
    logger.info("Dropping Outliers")
    idx = df['price'].between(args.min_price, args.max_price)
    df = df[idx].copy()
    
    idx = df['longitude'].between(-74.25, -73.50) & df['latitude'].between(40.5, 41.2)
    df = df[idx].copy()
    
    # Saving
    logger.info("Uploading output artifact to W&B")
    df.to_csv(args.output_artifact, index=False)

    artifact = wandb.Artifact(
        args.output_artifact,
        type=args.output_type,
        description=args.output_description,
    )
    artifact.add_file(args.output_artifact)
    run.log_artifact(artifact)
    artifact.wait()

if __name__ == "__main__":

    parser = argparse.ArgumentParser(description="A very basic data cleaning")


    parser.add_argument(
        "--input_artifact", 
        type=str,
        help='Name for the input data',
        required=True
    )

    parser.add_argument(
        "--output_artifact", 
        type=str,
        help='Name of the cleaned output data',
        required=True
    )

    parser.add_argument(
        "--output_type", 
        type=str,
        help='Type of the output data',
        required=True
    )

    parser.add_argument(
        "--output_description", 
        type=str,
        help='Description of the output data',
        required=True
    )

    parser.add_argument(
        "--min_price", 
        type=float,
        help='Mininum price available in the output data. Rows lower price will be dropped from output',
        required=True
    )

    parser.add_argument(
        "--max_price", 
        type=float,
        help='Maximum price available in the output data. Rows with greater price will be dropped from output',
        required=True
    )


    args = parser.parse_args()

    go(args)
