"""
Higher Education Advisory Services - LangGraph Agent

This agent orchestrates the full advisory call processing pipeline via
Unity Catalog function tools.
"""
import os
from typing import Any, Generator, Optional

import mlflow
from databricks_langchain import ChatDatabricks, UCFunctionToolkit
from langchain_core.runnables import RunnableLambda
from langgraph.graph import END, StateGraph
from mlflow.langchain.chat_agent_langgraph import ChatAgentState, ChatAgentToolNode
from mlflow.pyfunc import ChatAgent
from mlflow.types.agent import (
    ChatAgentChunk, ChatAgentMessage, ChatAgentResponse, ChatContext,
)

mlflow.langchain.autolog()

LLM_ENDPOINT_NAME = os.environ.get("AGENT_LLM_ENDPOINT", "databricks-claude-sonnet-4-6")
CATALOG = os.environ.get("CATALOG", "yyang")
SCHEMA = os.environ.get("SCHEMA", "contact_center_qa")

llm = ChatDatabricks(endpoint=LLM_ENDPOINT_NAME)

system_prompt = """You are an AI-powered advisor quality analyst for a Higher Education call center.

You help administrators and QA managers process, transcribe, and analyze advisory calls
(financial aid, admissions, enrollment, academic advising) at scale.

## Your Tools

### Discovery & File Management
1. **find_audio_file(speaker_query)** - Locate a specific speaker's audio file by name or number.
2. **find_all_audio_files()** - List every audio file in the advisory services Volume.

### Transcription
3. **transcribe_and_save_to_silver(file_path)** - Transcribe a single audio file with Whisper and return the transcript with metadata.
4. **process_all_audio_to_silver()** - Check transcription status: shows total files, already transcribed, and pending counts.

### Analysis (work on any transcript text)
5. **classify_call_category(transcription)** - Classify a call into: Financial Aid, Admissions, Enrollment, Academic Advising, Registration, Housing, Billing, Career Services, or Other.
6. **analyze_call_sentiment(transcription)** - Analyze student sentiment. Returns JSON with sentiment label and confidence.
7. **extract_topics_and_intent(transcription)** - Extract key topics, primary intent, and improvement areas.
8. **assess_rubric_rag(transcription)** - Score advisor performance 1-5 across 5 rubric criteria using RAG.
9. **enrich_single_call(transcription)** - Run ALL enrichment at once: sentiment, topics, category, and rubric assessment in one call.

### Pipeline Status
10. **enrich_silver_to_gold()** - Check enrichment pipeline status: silver vs gold record counts.

## Recommended Workflows

| User Request | Tool Sequence |
|---|---|
| "Transcribe speaker 12" | find_audio_file -> transcribe_and_save_to_silver |
| "Analyze this transcript" | enrich_single_call (or individual tools) |
| "Score this call" | assess_rubric_rag |
| "What files do we have?" | find_all_audio_files |
| "Pipeline status" | process_all_audio_to_silver -> enrich_silver_to_gold |
| "Full analysis of speaker 5" | find_audio_file -> transcribe_and_save_to_silver -> enrich_single_call |

## Guidelines
- Always confirm what was accomplished after each tool call.
- Report errors clearly with suggested remediation.
- For full call analysis, use enrich_single_call which runs all enrichment in one step.
- The rubric assessment scores advisors 1-5 across: Greeting, Active Listening, Accurate Information, Empathy, and Resolution.
"""

uc_tool_names = [
    f"{CATALOG}.{SCHEMA}.find_audio_file",
    f"{CATALOG}.{SCHEMA}.find_all_audio_files",
    f"{CATALOG}.{SCHEMA}.transcribe_and_save_to_silver",
    f"{CATALOG}.{SCHEMA}.process_all_audio_to_silver",
    f"{CATALOG}.{SCHEMA}.enrich_silver_to_gold",
    f"{CATALOG}.{SCHEMA}.classify_call_category",
    f"{CATALOG}.{SCHEMA}.analyze_call_sentiment",
    f"{CATALOG}.{SCHEMA}.extract_topics_and_intent",
    f"{CATALOG}.{SCHEMA}.assess_rubric_rag",
    f"{CATALOG}.{SCHEMA}.enrich_single_call",
]
uc_toolkit = UCFunctionToolkit(function_names=uc_tool_names)
tools = uc_toolkit.tools


def create_tool_calling_agent(model, tools, system_prompt=None):  # noqa: E303
    model = model.bind_tools(tools)

    def should_continue(state):
        last = state["messages"][-1]
        if hasattr(last, "tool_calls") and last.tool_calls:
            return "continue"
        if isinstance(last, dict) and last.get("tool_calls"):
            return "continue"
        return "end"

    if system_prompt:
        preprocessor = RunnableLambda(
            lambda state: [{"role": "system", "content": system_prompt}] + state["messages"]
        )
    else:
        preprocessor = RunnableLambda(lambda state: state["messages"])
    model_runnable = preprocessor | model

    def call_model(state, config):
        return {"messages": [model_runnable.invoke(state, config)]}

    workflow = StateGraph(ChatAgentState)
    workflow.add_node("agent", RunnableLambda(call_model))
    workflow.add_node("tools", ChatAgentToolNode(tools))
    workflow.set_entry_point("agent")
    workflow.add_conditional_edges(
        "agent", should_continue, {"continue": "tools", "end": END}
    )
    workflow.add_edge("tools", "agent")
    return workflow.compile()


class LangGraphChatAgent(ChatAgent):
    def __init__(self, agent):
        self.agent = agent

    def predict(self, messages, context=None, custom_inputs=None):
        request = {"messages": self._convert_messages_to_dict(messages)}
        out_msgs = []
        for event in self.agent.stream(request, stream_mode="updates"):
            for node_data in event.values():
                for m in node_data.get("messages", []):
                    if isinstance(m, dict):
                        out_msgs.append(ChatAgentMessage(**m))
                    else:
                        role = getattr(m, "type", "assistant")
                        role = "assistant" if role == "ai" else role
                        kwargs = {}
                        if getattr(m, "tool_calls", None):
                            kwargs["tool_calls"] = m.tool_calls
                        out_msgs.append(ChatAgentMessage(
                            role=role,
                            content=getattr(m, "content", str(m)),
                            **kwargs,
                        ))
        return ChatAgentResponse(messages=out_msgs)

    def predict_stream(self, messages, context=None, custom_inputs=None):
        request = {"messages": self._convert_messages_to_dict(messages)}
        for event in self.agent.stream(request, stream_mode="updates"):
            for node_data in event.values():
                for m in node_data.get("messages", []):
                    if isinstance(m, dict):
                        yield ChatAgentChunk(**{"delta": m})
                    else:
                        role = getattr(m, "type", "assistant")
                        role = "assistant" if role == "ai" else role
                        yield ChatAgentChunk(**{"delta": {
                            "role": role,
                            "content": getattr(m, "content", str(m)),
                        }})

agent = create_tool_calling_agent(llm, tools, system_prompt)
AGENT = LangGraphChatAgent(agent)
mlflow.models.set_model(AGENT)
