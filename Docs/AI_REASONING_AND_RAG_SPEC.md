# AI Architecture: Contextual Reasoning & RAG Integration

## Overview

This document defines the high-level architecture for integrating **Retrieval-Augmented Generation (RAG)** into the HorizonScale pipeline. This layer transitions the platform from a purely quantitative forecasting tool to an AI-driven **"Capacity Assistant"** capable of providing natural language explanations for resource trends and risks.

---

## 1. The RAG Framework for Infrastructure

Standard RAG pipelines typically query text documents. HorizonScale evolves this by implementing a **"Hybrid RAG"** that queries both structured Parquet data and unstructured metadata.

* **Structured Data Retrieval**: The system queries the Parquet master files to retrieve specific time-series metrics (CPU, Memory, Disk) for a given server ID.
* **Unstructured Context Retrieval**: A Vector Database (e.g., Pinecone or Milvus) stores engineering context, such as JIRA tickets, migration logs, and post-mortem incident reports.
* **Contextual Synthesis**: An LLM Agent retrieves both sets of data to explain "The Why" behind a predicted breach.

## 2. Technical Logic: Querying Parquet via LLM

To enable the AI to "read" the forecasting data, the architecture utilizes a SQL/Dataframe Agent approach.

1. **Natural Language Query**: A user asks: *"Why is the memory usage for server-2bad11d7 expected to peak in December?"*
2. **Metadata Fetch**: The agent retrieves the server’s specific forecast row from the Parquet file using a high-speed library like PyArrow or DuckDB.
3. **Vector Search**: The agent simultaneously searches the Vector DB for keywords related to that server and timeframe (e.g., "Memory Upgrade," "DB Migration").
4. **Reasoning Loop**: The LLM receives the forecast numbers (110% peak) and the JIRA context (e.g., "Project Alpha deployment") to generate a cohesive response.

## 3. Theoretical Implementation Roadmap

The transition to an agentic AI architecture follows a structured theoretical path:

* **Define Embedding Model**: Selection of an industry-standard model (e.g., OpenAI `text-embedding-3-small` or HuggingFace alternatives) to vectorize historical JIRA and log data into high-dimensional space.
* **Initialize Vector DB**: Theoretical setup of a specialized database (Pinecone or Milvus) to act as the long-term "memory" for infrastructure context.
* **Prototype Agent**: Development of a reasoning agent using LangChain or LlamaIndex capable of executing the "Structured-to-Unstructured" hybrid query.

---

## 4. Proposed Metadata Schema

To support RAG, the following fields must be indexed in the Vector Database:

| Field | Type | Description |
| --- | --- | --- |
| **Server_ID** | String (Primary Key) | Matches the ID in the Parquet master file. |
| **Event_Type** | Categorical | Migration, Patching, Release, Outage. |
| **Event_Summary** | Text | The "Reason" extracted from JIRA or logs. |
| **Impact_Window** | Timestamp | Start and End dates for the event. |

