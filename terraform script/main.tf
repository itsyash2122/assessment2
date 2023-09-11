provider "aws" {
  region = "us-west-2" # Change to your desired AWS region
}

resource "aws_vpc" "assessment2" {
  cidr_block = "10.0.0.0/16" # Change the CIDR block as needed
  tags= {
    Name="crc vpc",
    Task="Assessment2"

  }
}

resource "aws_subnet" "assessment2" {

  availability_zone = "us-west-2b" # Change to your desired AZ
  vpc_id           = aws_vpc.assessment2.id
  cidr_block       = "10.0.1.0/24" # Change the CIDR block as needed

    tags= {
    Name="crc subnet",
    Task="Assessment2"

  }
}

resource "aws_subnet" "assessment2-2" {

  availability_zone = "us-west-2a" # Change to your desired AZ
  vpc_id           = aws_vpc.assessment2.id
  cidr_block       = "10.0.2.0/24" # Change the CIDR block as needed

    tags= {
    Name="crc subnet2",
    Task="Assessment2"

  }
}

resource "aws_iam_role" "assessment2" {
  name = "assessment2-eks-node-role"

  assume_role_policy = <<EOF
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": {
        "Service": "ec2.amazonaws.com"
      },
      "Action": "sts:AssumeRole"
    }
  ]
}
EOF
}

resource "aws_iam_role_policy_attachment" "assessment2" {
  policy_arn = "arn:aws:iam::aws:policy/AmazonEKSWorkerNodePolicy"
  role       = aws_iam_role.assessment2.name

    tags= {
    Name="crc iamrolepolicy1",
    Task="Assessment2"

  }
}

resource "aws_iam_role_policy_attachment" "assessment2-2" {
  policy_arn = "arn:aws:iam::aws:policy/AmazonEC2ContainerRegistryReadOnly"
  role       = aws_iam_role.assessment2.name

      tags= {
    Name="crc iamrolepolicy2",
    Task="Assessment2"

  }
}

resource "aws_iam_role_policy_attachment" "assessment2-3" {
  policy_arn = "arn:aws:iam::aws:policy/AmazonEKSClusterPolicy"
  role       = aws_iam_role.assessment2.name

      tags= {
    Name="crc iamrolepolicy3",
    Task="Assessment2"

  }
}



resource "aws_eks_cluster" "assessment2" {
  name     = "assessment2"
  role_arn = aws_iam_role.assessment2.arn

  vpc_config {
    subnet_ids = [ aws_subnet.assessment2.id, aws_subnet.assessment2-2.id ]
  }

  depends_on = [
    aws_iam_role_policy_attachment.assessment2,
  ]


      tags= {
    Name="crc cluster",
    Task="Assessment2"
  }
}

resource "aws_eks_node_group" "assessment2" {
  cluster_name    = aws_eks_cluster.assessment2.name
  node_group_name = "assessment2-workers"
  node_role_arn   = aws_iam_role.assessment2.arn
  subnet_ids = [aws_subnet.assessment2.id,aws_subnet.assessment2-2.id]


  scaling_config {
    desired_size = 1
    max_size     = 1
    min_size     = 1
  }


      tags= {
    Name="crc nodegroup",
    Task="Assessment2"

  }
}



