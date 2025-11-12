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


public class App {

  private static String loadSystemPrompt() {
    try (InputStream inputStream = App.class.getResourceAsStream("/system-prompt.txt")) {
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
    try (InputStream inputStream = App.class.getResourceAsStream("/response-schema.json")) {
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
   * Appelle le modèle LLM avec l'entrée fournie et renvoie la réponse textuelle.
   *
   * Prérequis: la variable d'environnement GEMINI_API_KEY doit être définie.
   *
   * @param input Texte d'entrée utilisateur à envoyer au modèle
   * @return La réponse du LLM (texte). Si aucun texte n'est retourné, renvoie une chaîne vide.
   */
  public static String generateResponse(String input) {
    String apiKey = System.getenv("GEMINI_API_KEY");
    Client client = Client.builder().apiKey(apiKey).build();

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
        .thinkingConfig(
          ThinkingConfig
            .builder()
            .thinkingBudget(0)
            .build()
        )
        .imageConfig(
          ImageConfig
            .builder()
            .imageSize("1K")
            .build()
        )
        .responseMimeType("application/json")
        .responseSchema(Schema.fromJson(loadResponseSchema()))
        .systemInstruction(
          Content.fromParts(
            Part.fromText(loadSystemPrompt())
          )
        )
        .build();

    GenerateContentResponse res = client.models.generateContent(model, contents, config);

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
}
