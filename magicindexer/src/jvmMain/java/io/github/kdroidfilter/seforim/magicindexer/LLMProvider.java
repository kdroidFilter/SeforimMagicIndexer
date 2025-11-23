package io.github.kdroidfilter.seforim.magicindexer;

import com.google.common.collect.ImmutableList;
import com.google.genai.Client;
import com.google.genai.types.*;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.stream.Collectors;


public class LLMProvider {

  private static String loadSystemPrompt() {
    try (InputStream inputStream = LLMProvider.class.getResourceAsStream("/system-prompt.txt")) {
      if (inputStream == null) {
        throw new RuntimeException("Could not find system-prompt.txt in resources");
      }
      try (BufferedReader reader = new BufferedReader(
          new InputStreamReader(inputStream, StandardCharsets.UTF_8))) {
        return reader.lines().collect(Collectors.joining("\n"));
      }
    } catch (IOException e) {
      throw new RuntimeException("Failed to load system prompt from resources", e);
    }
  }

  private static String loadResponseSchema() {
    try (InputStream inputStream = LLMProvider.class.getResourceAsStream("/response-schema.json")) {
      if (inputStream == null) {
        throw new RuntimeException("Could not find response-schema.json in resources");
      }
      try (BufferedReader reader = new BufferedReader(
          new InputStreamReader(inputStream, StandardCharsets.UTF_8))) {
        return reader.lines().collect(Collectors.joining("\n"));
      }
    } catch (IOException e) {
      throw new RuntimeException("Failed to load response schema from resources", e);
    }
  }

    /**
     * Calls the LLM model with the provided input and returns the text response.
     * Prerequisite: The GEMINI_API_KEY environment variable must be set.
     * @param input User input text to send to the model
     * @param timeoutSeconds Timeout in seconds for the API call
     * @return The LLM response (text). If no text is returned, returns an empty string.
     */
  public static String generateResponse(String input, int timeoutSeconds) {
    String apiKey = System.getenv("GEMINI_API_KEY");

    // Configure HTTP timeout (Note: timeout may not work in versions 1.8.0+)
    HttpOptions httpOptions = HttpOptions.builder()
        .timeout(timeoutSeconds * 1000)  // timeout in milliseconds
        .build();

    Client client = Client.builder()
        .apiKey(apiKey)
        .httpOptions(httpOptions)
        .build();

    String model = "gemini-2.5-flash";
    List<Content> contents = ImmutableList.of(
      Content.builder()
        .role("user")
        .parts(ImmutableList.of(
          Part.fromText(input)
        ))
        .build()
    );

    GenerateContentConfig config =
      GenerateContentConfig
        .builder()
        .temperature(0f)
        // Allow larger JSON responses to avoid truncation
        .maxOutputTokens(30000)
        .responseMimeType("application/json")
        .responseSchema(Schema.fromJson(loadResponseSchema()))
        .systemInstruction(
          Content.fromParts(
            Part.fromText(loadSystemPrompt())
          )
        )
        .build();

    GenerateContentResponse res;
    try {
      res = client.models.generateContent(model, contents, config);
    } catch (Exception e) {
      System.err.println("Error calling LLM API: " + e.getMessage());
      if (e instanceof java.net.SocketTimeoutException ||
          (e.getMessage() != null && e.getMessage().contains("timeout"))) {
        System.err.println("Request timed out after " + timeoutSeconds + " seconds");
      }
      return "";
    }

    if (res.candidates().isEmpty() ||
        res.candidates().get().get(0).content().isEmpty() ||
        res.candidates().get().get(0).content().get().parts().isEmpty()) {
      return "";
    }

    StringBuilder sb = new StringBuilder();
    List<Part> parts = res.candidates().get().get(0).content().get().parts().get();
    for (Part part : parts) {
      if (part.text().isPresent()) {
        sb.append(part.text().get());
      }
    }
    return sb.toString();
  }

  /**
   * Calls the LLM model with the provided input and returns the text response.
   * Uses default timeout of 40 seconds.
   * @param input User input text to send to the model
   * @return The LLM response (text). If no text is returned, returns an empty string.
   */
  public static String generateResponse(String input) {
    return generateResponse(input, 40);
  }
}
